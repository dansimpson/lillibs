package com.github.dansimpson.lilkv.serde;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.github.dansimpson.lilkv.KvStore;
import com.github.dansimpson.lilkv.serde.Serde.SerdeException;
import com.github.dansimpson.lilkv.util.Try;

public class SerdeKvStore<K, V> implements Closeable {

	private final KvStore store;
	private final Serde<K> keySerde;
	private final Serde<V> valueSerde;

	public SerdeKvStore(KvStore store, Serde<K> keySerde, Serde<V> valueSerde) {
		super();
		this.store = store;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
	}

	/**
	 * Set a given key to a value
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @param data
	 *          The raw data which to store
	 * @return The previous value of the data, which may equal the data value, or be null if there was no record before.
	 */
	public Try<V> set(K key, V value) {
		return Try.attemptTry(() -> store.set(keySerde.serialize(key), valueSerde.serialize(value))).map(this::mapValue);
	}

	/**
	 * Get a key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return The value, or null, or a Failure if the underlying store experienced an issue
	 */
	public Try<V> get(K key) {
		return Try.attempt(() -> keySerde.serialize(key)).flatMap(k -> store.get(k)).map(this::mapValue);
	}

	/**
	 * Delete a key
	 * 
	 * @param key
	 *          the unique key which identifies the data
	 * @return A try with the previous value, or null, or a failure when the underlying system failed to execute the command.
	 */
	public Try<V> delete(K key) {
		return Try.attempt(() -> keySerde.serialize(key)).flatMap(k -> store.delete(k)).map(this::mapValue);
	}

	/**
	 *
	 * @return A stream of keys, or a Failure when the underlying storage failed to fetch keys
	 */
	public Stream<Entry<K, V>> entries() {
		return store.entries().map(item -> {
			try {
				return new SimpleEntry<K, V>(keySerde.deserialize(item.getKey()), valueSerde.deserialize(item.getValue()));
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		});
	}

	private V mapValue(byte[] data) throws SerdeException {
		if (data == null) {
			return null;
		}
		return valueSerde.deserialize(data);
	}

	@Override
	public void close() throws IOException {
		store.close();
	}

}
