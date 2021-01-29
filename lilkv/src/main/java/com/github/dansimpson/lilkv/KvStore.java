package com.github.dansimpson.lilkv;

import java.io.Closeable;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.github.dansimpson.lilkv.util.Try;

/**
 * Interface which all KvStore implementations must abide. The dude abides.
 * 
 * @author Dan Simpson
 *
 */
public interface KvStore extends Closeable {

	public static class KeyNotFoundError extends Exception {

		private static final long serialVersionUID = -1234157807295155413L;

		public KeyNotFoundError() {
			super("Key not found");
		}
	}

	public static class ValueUnchangedError extends Exception {

		private static final long serialVersionUID = 3911891486701183827L;

		public ValueUnchangedError() {
			super("Value unchanged");
		}
	}

	public static class StaleClockError extends Exception {

		private static final long serialVersionUID = -4827399029965824911L;

		public StaleClockError() {
			super("Stale clock");
		}
	}

	public static final KeyNotFoundError ERR_KEY_NOT_FOUND = new KeyNotFoundError();
	public static final ValueUnchangedError ERR_VALUE_UNCHANGED = new ValueUnchangedError();
	public static final StaleClockError ERR_STALE_CLOCK = new StaleClockError();
	public static final byte[] EMPTY_BYTES = new byte[0];

	/**
	 * Set a given key to a value
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @param data
	 *          The raw data which to store
	 * @return A try with the previous value, or failure if the key doesn't exist, the value didn't change, or there was an error
	 */
	public Try<byte[]> set(byte[] key, byte[] data);

	/**
	 * Get a value for a given key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return A try object containing the value, or failure if the key doesn't exist or there was an error
	 */
	public Try<byte[]> get(byte[] key);

	/**
	 * Delete a key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return A try with the previous value, or failure if the key doesn't exist or there was an error
	 */
	public Try<byte[]> delete(byte[] key);

	/**
	 * 
	 * @return A stream of all key value pairs
	 */
	public Stream<Entry<byte[], byte[]>> entries();

}
