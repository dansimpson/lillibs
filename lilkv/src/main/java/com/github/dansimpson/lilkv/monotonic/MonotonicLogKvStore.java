package com.github.dansimpson.lilkv.monotonic;

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
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilkv.KvStore;
import com.github.dansimpson.lilkv.LogKvStore;
import com.github.dansimpson.lilkv.LogKvStore.ByteArrayComparator;
import com.github.dansimpson.lilkv.util.Try;

public class MonotonicLogKvStore implements MonotonicKvStore {

	private static final Logger log = LoggerFactory.getLogger(LogKvStore.class);

	protected static final byte VERSION = 1;
	protected static final byte SET_COMMAND = 1;
	protected static final byte DELETE_COMMAND = 2;

	private static final class Command {

		private final byte code;
		private final byte[] key;
		private final long clock;
		private final byte[] value;

		private Command(byte code, byte[] key, long clock, byte[] value) {
			super();
			this.code = code;
			this.key = key;
			this.clock = clock;
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
				long clock = buffer.getLong();
				byte[] key = new byte[buffer.getShort()];
				buffer.get(key);
				byte[] value = new byte[buffer.getInt()];
				buffer.get(value);
				callback.accept(new Command(header.get(1), key, clock, value));
				header.rewind();
			}
		}

		private static final int HEADER_SIZE = 6;
		private static final int LLI_SIZE = 6 + 8;
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
			buffer.putLong(command.clock);
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

	private final TreeMap<byte[], ClockedKeyValue> cache = new TreeMap<>(new ByteArrayComparator());
	private long compactionCount = 0;

	private final int minCompactionCount;
	private final double compactionThreshold;

	private FileOutputStream output;

	public MonotonicLogKvStore(Path directory) throws IOException {
		this(directory, 1000, 50);
	}

	public MonotonicLogKvStore(Path directory, int minCompactionCount, int compactionThresholdPercentage)
	    throws IOException {
		assert (compactionThresholdPercentage >= 0);
		assert (compactionThresholdPercentage <= 100);
		this.directory = directory;

		this.storeFile = directory.resolve("monostore.dat").toFile();
		this.backupFile = directory.resolve("monostore.backup").toFile();
		this.compactFile = directory.resolve("monocompact.dat").toFile();

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
	public synchronized Try<byte[]> set(byte[] key, long clock, byte[] value) {
		ClockedKeyValue previous = cache.get(key);

		if (previous != null) {
			if (previous.getClock() >= clock) {
				return Try.failure(KvStore.ERR_STALE_CLOCK);
			}

			if (Arrays.equals(previous.getValue(), value)) {
				return Try.failure(KvStore.ERR_VALUE_UNCHANGED);
			}
		}

		return Try.attempt(() -> issue(new Command(SET_COMMAND, key, clock, value))).map((v) -> {
			cache.put(key, new ClockedKeyValue(key, value, clock));
			if (previous != null) {
				compactionCount++;
				return previous.getValue();
			}
			return null;
		});
	}

	@Override
	public Try<byte[]> get(byte[] key) {
		ClockedKeyValue value = cache.get(key);
		if (value == null) {
			return Try.failure(KvStore.ERR_KEY_NOT_FOUND);
		}
		return Try.success(value.getValue());
	}

	@Override
	public synchronized Try<byte[]> delete(byte[] key, long clock) {
		ClockedKeyValue current = cache.get(key);
		if (current == null) {
			return Try.failure(KvStore.ERR_KEY_NOT_FOUND);
		}

		if (current.getClock() >= clock) {
			return Try.failure(KvStore.ERR_STALE_CLOCK);
		}

		return Try.attempt(() -> issue(new Command(DELETE_COMMAND, key, clock, KvStore.EMPTY_BYTES))).map((v) -> {
			compactionCount++;
			return cache.remove(key).getValue();
		});
	}

	@Override
	public Stream<ClockedKeyValue> entries() {
		return cache.values().stream();
	}

	@Override
	public Try<Long> getClock(byte[] key) {
		ClockedKeyValue value = cache.get(key);
		if (value == null) {
			return Try.failure(KvStore.ERR_KEY_NOT_FOUND);
		}
		return Try.success(value.getClock());
	}

	@Override
	public Stream<ClockedKeyValue> entriesBetween(long lowerClock, long upperClock, boolean inclusiveLower,
	    boolean inclusiveUpper) {
		Predicate<ClockedKeyValue> lower = inclusiveLower ? ((e) -> e.getClock() >= lowerClock)
		    : ((e) -> e.getClock() > lowerClock);
		Predicate<ClockedKeyValue> upper = inclusiveUpper ? ((e) -> e.getClock() <= upperClock)
		    : ((e) -> e.getClock() < upperClock);
		return entries().filter(lower.and(upper));
	}

	/**
	 *
	 * @return The resolved path of the DB file
	 */
	public Path getDbFilePath() {
		return storeFile.toPath();
	}

	//////////////////////
	// Private
	//////////////////////

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

	private synchronized void rebuildCache(File db) throws IOException {
		try (SeekableByteChannel channel = Files.newByteChannel(db.toPath(), StandardOpenOption.READ)) {
			Command.decode(channel, (command) -> {
				if (command.code == SET_COMMAND) {
					if (cache.put(command.key, new ClockedKeyValue(command.key, command.value, command.clock)) != null) {
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
			for (ClockedKeyValue ckv : cache.values()) {
				compact.write(Command.encode(new Command(SET_COMMAND, ckv.getKey(), ckv.getClock(), ckv.getValue())));
			}
			compact.flush();
		}

		Files.move(storeFile.toPath(), backupFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
		Files.move(compactFile.toPath(), storeFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
		Files.delete(backupFile.toPath());

		output = new FileOutputStream(storeFile, true);
	}

}
