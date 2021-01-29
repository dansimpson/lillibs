package com.github.dansimpson.lilkv.monotonic;

import java.io.Closeable;
import java.util.stream.Stream;

import com.github.dansimpson.lilkv.util.Try;

public interface MonotonicKvStore extends Closeable {

	public static final class ClockedKeyValue {

		private final byte[] key;
		private final byte[] value;
		private final long clock;

		/**
		 * @param key
		 * @param value
		 * @param clock
		 */
		public ClockedKeyValue(byte[] key, byte[] value, long clock) {
			super();
			this.key = key;
			this.value = value;
			this.clock = clock;
		}

		/**
		 * @return the key byte[]
		 */
		public byte[] getKey() {
			return key;
		}

		/**
		 * @return the value byte[]
		 */
		public byte[] getValue() {
			return value;
		}

		/**
		 * @return the clock value
		 */
		public long getClock() {
			return clock;
		}

	}

	/**
	 * Set a given key to a value
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @param clock
	 *          the monotonic clock for the key, which will only apply if the previous clock is less than this clock
	 * @param data
	 *          The raw data which to store
	 * @return A try with the previous value, or failure if the key doesn't exist, the value didn't change, or there was an error
	 */
	public Try<byte[]> set(byte[] key, long clock, byte[] data);

	/**
	 * Get a value for a given key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return A try object containing the value, or failure if the key doesn't exist or there was an error
	 */
	public Try<byte[]> get(byte[] key);

	/**
	 * Get the clock value for a given key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return A try object containing the clock, or failure if the key doesn't exist or there was an error
	 */
	public Try<Long> getClock(byte[] key);

	/**
	 * Delete a key, only if the key is present, and the current clock is less than the passed clock
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @param clock
	 *          the monotonic clock for the key, which will only apply if the previous clock is less than this clock
	 * @return A try with the previous value, or null, or a failure when the underlying system failed to execute the command.
	 */
	public Try<byte[]> delete(byte[] key, long clock);

	/**
	 * Fetch a stream of entries
	 * 
	 * @return A stream of all key value pairs
	 */
	public Stream<ClockedKeyValue> entries();

	/**
	 * Fetch a filtered stream of entries with clocks in a given range
	 * 
	 * @param lowerClock
	 *          the lower clock value
	 * @param upperClock
	 *          the upper clock value
	 * @param inclusiveLower
	 *          flag to include the lower clock value, eg: clock >= lowerClock if true
	 * @param inclusiveUpper
	 *          flag to include the upper clock value, eg: clock <= upperClock if true
	 * @return A stream of key value pairs
	 */
	public Stream<ClockedKeyValue> entriesBetween(long lowerClock, long upperClock, boolean inclusiveLower,
	    boolean inclusiveUpper);

	/**
	 * Fetch a filtered stream of entries with clocks in a given range (exclusive)
	 * 
	 * @param lowerClock
	 *          the lower clock value
	 * @param upperClock
	 *          the upper clock value
	 * @return A stream of key value pairs
	 */
	public default Stream<ClockedKeyValue> entriesBetween(long lowerClock, long upperClock) {
		return entriesBetween(lowerClock, upperClock, false, false);
	}

	/**
	 * Fetch a filtered stream of entries with clocks greater than the passed clock
	 * 
	 * @param clock
	 *          the lower clock value
	 * @param inclusive
	 *          flag to include the lower clock value, eg: clock >= lowerClock if true
	 * @return A stream of key value pairs
	 */
	public default Stream<ClockedKeyValue> entriesAfter(long clock, boolean inclusive) {
		return entriesBetween(clock, Long.MAX_VALUE, inclusive, false);
	}

	public default Stream<ClockedKeyValue> entriesAfter(long clock) {
		return entriesAfter(clock, false);
	}

}
