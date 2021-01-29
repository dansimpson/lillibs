package com.github.dansimpson.lilkv;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilkv.util.Try;

/**
 * A Key value store which writes every mutative action to a log file. The log file is periodically compacted based on compaction
 * parameters. This compaction currently isn't asynchronous and will block mutative actions while running.
 * 
 * @author Dan Simpson
 *
 */
public class LogKvStore implements KvStore {

	public static final class ByteArrayComparator implements Comparator<byte[]> {

		@Override
		public int compare(byte[] b1, byte[] b2) {
			if (b1.length != b2.length) {
				return b1.length > b2.length ? 1 : -1;
			}
			for (int i = 0; i < b1.length; i++) {
				if (b1[i] != b2[i]) {
					return (b1[i] & 0xFF) > (b2[i] & 0xFF) ? 1 : -1;
				}
			}
			return 0;
		}
	}

	private static final Logger log = LoggerFactory.getLogger(LogKvStore.class);

	protected static final byte VERSION = 1;
	protected static final byte SET_COMMAND = 1;
	protected static final byte DELETE_COMMAND = 2;

	private static final class Command {

		private byte code;
		private byte[] key;
		private byte[] value;

		private Command(byte code, byte[] key, byte[] value) {
			super();
			this.code = code;
			this.key = key;
			this.value = value;
		}

		private static void decode(SeekableByteChannel channel, Consumer<Command> callback) throws IOException {
			ByteBuffer header = ByteBuffer.allocate(6);

			while (channel.read(header) == 6) {
				ByteBuffer buffer = ByteBuffer.allocate(header.getInt(2));

				if (channel.read(buffer) != header.getInt(2)) {
					throw new IOException(String.format("Corrupt file! Failed to read %d bytes!", header.getInt(2)));
				}

				buffer.rewind();
				byte[] key = new byte[buffer.getShort()];
				buffer.get(key);

				byte[] value = new byte[buffer.getInt()];
				buffer.get(value);

				callback.accept(new Command(header.get(1), key, value));

				header.rewind();
			}
		}

		private static final int HEADER_SIZE = 6;
		private static final int LLI_SIZE = 6;
		private static final int META_SIZE = HEADER_SIZE + LLI_SIZE;

		private static byte[] encode(Command command) {
			int keySize = command.key.length;
			int valSize = command.value.length;

			assert (keySize < Short.MAX_VALUE);
			assert (valSize < Integer.MAX_VALUE);

			ByteBuffer buffer = ByteBuffer.allocate(keySize + valSize + META_SIZE);
			buffer.put(VERSION);
			buffer.put(command.code);
			buffer.putInt(keySize + valSize + LLI_SIZE);
			buffer.putShort((short) command.key.length);
			buffer.put(command.key);
			buffer.putInt(command.value.length);
			buffer.put(command.value);
			return buffer.array();
		}

	}

	private final Path directory;
	private final File storeFile;
	private final File backupFile;
	private final File compactFile;

	private final TreeMap<byte[], byte[]> cache = new TreeMap<>(new ByteArrayComparator());
	private long compactionCount = 0;

	private final int minCompactionCount;
	private final double compactionThreshold;

	private FileOutputStream output;

	public LogKvStore(Path directory) throws IOException {
		this(directory, 1000, 50);
	}

	public LogKvStore(Path directory, int minCompactionCount, int compactionThresholdPercentage) throws IOException {
		assert (compactionThresholdPercentage >= 0);
		assert (compactionThresholdPercentage <= 100);
		this.directory = directory;

		this.storeFile = directory.resolve("store.dat").toFile();
		this.backupFile = directory.resolve("store.backup").toFile();
		this.compactFile = directory.resolve("compact.dat").toFile();

		this.minCompactionCount = minCompactionCount;
		this.compactionThreshold = compactionThresholdPercentage / 100d;
		this.open();
	}

	@Override
	public synchronized void close() throws IOException {
		if (output != null) {
			output.close();
		}
	}

	@Override
	public synchronized Try<byte[]> set(byte[] key, byte[] value) {
		byte[] previous = cache.get(key);

		if (Arrays.equals(previous, value)) {
			return Try.failure(ERR_VALUE_UNCHANGED);
		}

		return Try.attempt(() -> issue(new Command(SET_COMMAND, key, value))).map((v) -> {
			cache.put(key, value);
			if (previous != null) {
				compactionCount++;
				return previous;
			}
			return null;
		});
	}

	@Override
	public Try<byte[]> get(byte[] key) {
		byte[] value = cache.get(key);
		if (value == null) {
			return Try.failure(ERR_KEY_NOT_FOUND);
		}
		return Try.success(value);
	}

	@Override
	public synchronized Try<byte[]> delete(byte[] key) {
		if (!cache.containsKey(key)) {
			return Try.failure(ERR_KEY_NOT_FOUND);
		}

		return Try.attempt(() -> issue(new Command(DELETE_COMMAND, key, EMPTY_BYTES))).map((v) -> {
			compactionCount++;
			return cache.remove(key);
		});
	}

	@Override
	public Stream<Entry<byte[], byte[]>> entries() {
		return cache.entrySet().stream();
	}

	private synchronized void open() throws IOException {
		File dir = directory.toFile();

		// Setup the directory if needed
		if (!dir.exists()) {
			Files.createDirectories(directory);
		}

		// We had a rare failure
		if (!storeFile.exists() && backupFile.exists()) {
			log.warn("Found a backup file and no db file... trying to restore!");
			Files.move(backupFile.toPath(), storeFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
		}

		// Check for active kv file
		if (storeFile.exists() && cache.isEmpty()) {
			rebuildCache(storeFile);
		}

		output = new FileOutputStream(storeFile, true);
	}

	private void rebuildCache(File db) throws IOException {
		try (SeekableByteChannel channel = Files.newByteChannel(db.toPath(), StandardOpenOption.READ)) {
			Command.decode(channel, (command) -> {
				if (command.code == SET_COMMAND) {
					if (cache.put(command.key, command.value) != null) {
						compactionCount++;
					}
				} else if (command.code == DELETE_COMMAND) {
					cache.remove(command.key);
					compactionCount++;
				}
			});
		}
	}

	private synchronized boolean issue(Command command) throws IOException {
		output.write(Command.encode(command));
		output.flush();
		if (isCompactionRequired()) {
			// We don't actually want to "Fail" if compaction failed. This doesn't impact the availability of the data. Logging is important
			// however, since we want some way to inform the operator of an issue.
			try {
				compact();
				return true;
			} catch (IOException exception) {
				log.error("Failed to compact KV file in dir: {}, reason: {}", directory.toString(), exception.getMessage());
				log.debug("Detail", exception);

				// We need to try and recover, re-open the original... if that fails, we need to send it upstream, as we are
				// now in a state where availability isn't guaranteed.
				open();
			}
		}
		return false;
	}

	private boolean isCompactionRequired() {
		if (compactionCount < minCompactionCount) {
			return false;
		}

		double total = cache.size() + compactionCount;
		if (total == 0) {
			return false;
		}

		return (compactionCount / total) >= compactionThreshold;
	}

	/**
	 * Atomically compact the store to a new log, with all tombstones purged. If this fails mid-way through, the store should be able recover
	 * via the backup.
	 * 
	 * @throws IOException
	 */
	private synchronized void compact() throws IOException {
		close();

		try (FileOutputStream compact = new FileOutputStream(compactFile, false)) {
			for (byte[] key : cache.keySet()) {
				compact.write(Command.encode(new Command(SET_COMMAND, key, cache.get(key))));
			}
			compact.flush();
		}

		Files.move(storeFile.toPath(), backupFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
		Files.move(compactFile.toPath(), storeFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
		Files.delete(backupFile.toPath());

		output = new FileOutputStream(storeFile, true);
	}

	public Path getDbFilePath() {
		return storeFile.toPath();
	}

}
