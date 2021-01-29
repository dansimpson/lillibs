package com.github.dansimpson.lilkv.replicated;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.ClusterResponse;
import com.github.dansimpson.lilkv.serde.Serde;
import com.github.dansimpson.lilkv.serde.Serde.SerdeException;
import com.github.dansimpson.lilkv.util.Try;

/**
 * A keyspace with defined Serdes for keys and values. Useful for one or more namespaces. This essentially allows simple serialization and
 * deserialization of keys and values to domain models. Unless working with byte array directly is desired, these should be leveraged. This
 * class should be instantiated via ReplicatedKvStore.keyspace() which returns a builder.
 * 
 * @see { ReplicatedKvStore.keyspace }
 * 
 * @author Dan Simpson
 *
 * @param <K>
 * @param <V>
 */
public class ReplicatedKeySpace<K, V> implements ReplicatedKvStoreListener, Closeable {

	private static final Logger log = LoggerFactory.getLogger(ReplicatedKeySpace.class);

	private final String namespace;
	private final Serde<K> keySerde;
	private final Serde<V> valueSerde;
	private final ReplicatedKvStore store;

	private final BiConsumer<K, V> onChange;
	private final Consumer<K> onDelete;

	/**
	 * @param namespace
	 * @param keySerde
	 * @param valueSerde
	 * @param store
	 * @param onChange
	 * @param onDelete
	 */
	private ReplicatedKeySpace(ReplicatedKvStore store, String namespace, Serde<K> keySerde, Serde<V> valueSerde,
	    BiConsumer<K, V> onChange, Consumer<K> onDelete) {
		super();
		this.namespace = namespace;
		this.keySerde = keySerde;
		this.valueSerde = valueSerde;
		this.store = store;
		this.onChange = onChange;
		this.onDelete = onDelete;

		store.listen(namespace, this);
	}

	@Override
	public void close() throws IOException {
		store.stopListening(namespace, this);
	}

	public Try<CompletableFuture<ClusterResponse>> set(K key, V value) {
		return Try.attemptTry(() -> {
			return store.put(namespace, keySerde.serialize(key), valueSerde.serialize(value));
		});
	}

	public Try<CompletableFuture<ClusterResponse>> delete(K key) {
		return Try.attempt(() -> keySerde.serialize(key)).flatMap(k -> store.delete(namespace, k));
	}

	public Try<V> get(K key) {
		return Try.attempt(() -> keySerde.serialize(key))
		    .flatMap(k -> store.get(namespace, k))
		    .map(data -> valueSerde.deserialize(data));
	}

	public Stream<Entry<K, V>> entries() {
		return store.entries(namespace).map(item -> {
			try {
				return new SimpleEntry<K, V>(keySerde.deserialize(item.getKey()), valueSerde.deserialize(item.getValue()));
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		});
	}

	@Override
	public void onUpdate(byte[] key, byte[] value) {
		try {
			onChange.accept(keySerde.deserialize(key), valueSerde.deserialize(value));
		} catch (SerdeException e) {
			log.error("Failed to deserialize key/values!", e);
		}
	}

	@Override
	public void onDelete(byte[] key) {
		try {
			onDelete.accept(keySerde.deserialize(key));
		} catch (SerdeException e) {
			log.error("Failed to deserialize key/values!", e);
		}
	}

	public static class Builder<K, V> {

		private final String keyspace;
		private final ReplicatedKvStore store;

		private Serde<K> keySerde;
		private Serde<V> valueSerde;

		private BiConsumer<K, V> onChange = (k, v) -> {
		};
		private Consumer<K> onDelete = (k) -> {
		};

		public Builder<K, V> onChange(BiConsumer<K, V> callback) {
			this.onChange = callback;
			return this;
		}

		public Builder<K, V> onDelete(Consumer<K> callback) {
			this.onDelete = callback;
			return this;
		}

		/**
		 * @param namespace
		 * @param store
		 */
		protected Builder(ReplicatedKvStore store, String keyspace, Serde<K> keySerde, Serde<V> valueSerde) {
			super();
			this.keyspace = keyspace;
			this.store = store;
			this.keySerde = keySerde;
			this.valueSerde = valueSerde;
		}

		public ReplicatedKeySpace<K, V> build() {
			return new ReplicatedKeySpace<K, V>(store, keyspace, keySerde, valueSerde, onChange, onDelete);
		}

	}

}
