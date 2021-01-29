package com.github.dansimpson.lilkv.replicated;

import static com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.REQ_DEL;
import static com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.REQ_SET;
import static com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.REQ_STATE;
import static com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.REQ_SYNC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.Cluster;
import com.github.dansimpson.lilcluster.ClusterResponse;
import com.github.dansimpson.lilcluster.Peer;
import com.github.dansimpson.lilkv.LogKvStore;
import com.github.dansimpson.lilkv.monotonic.MonotonicKvStore;
import com.github.dansimpson.lilkv.monotonic.MonotonicKvStore.ClockedKeyValue;
import com.github.dansimpson.lilkv.monotonic.MonotonicLogKvStore;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.AckResponse;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.Delete;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.KvOp;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.SendBulk;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.Set;
import com.github.dansimpson.lilkv.replicated.ReplicatedKvProto.SyncRequest;
import com.github.dansimpson.lilkv.serde.LongSerde;
import com.github.dansimpson.lilkv.serde.Serde;
import com.github.dansimpson.lilkv.serde.SerdeKvStore;
import com.github.dansimpson.lilkv.serde.StringSerde;
import com.github.dansimpson.lilkv.util.Try;

public class ReplicatedKvStore {

	private static final Logger log = LoggerFactory.getLogger(ReplicatedKvStore.class);

	private final String storageDir;
	private final Cluster cluster;
	private final Map<String, MonotonicKvStore> keySpaces = new ConcurrentHashMap<>();

	private final Map<String, CopyOnWriteArraySet<ReplicatedKvStoreListener>> listeners = new ConcurrentHashMap<>();

	private final SerdeKvStore<String, String> keySpaceMeta;
	private final SerdeKvStore<String, Long> peerCheckins;

	private final AtomicBoolean emptyStore = new AtomicBoolean(false);

	public ReplicatedKvStore(String storageDir, Cluster.Builder builder) throws IOException {
		this.storageDir = storageDir;

		// Wire up the handlers for the cluster
		builder.onPeerRequest(this::onPeerRequest);
		builder.onPeerSend(this::onPeerSend);
		builder.onPeerJoin(this::onPeerJoin);
		cluster = builder.build();

		keySpaceMeta = new SerdeKvStore<>(new LogKvStore(Paths.get(storageDir, ".keyspaces")), new StringSerde(),
		    new StringSerde());
		peerCheckins = new SerdeKvStore<>(new LogKvStore(Paths.get(storageDir, ".checkins")), new StringSerde(),
		    new LongSerde());
	}

	public void listen(String keyspace, ReplicatedKvStoreListener listener) {
		listeners.computeIfAbsent(keyspace, (k) -> new CopyOnWriteArraySet<ReplicatedKvStoreListener>()).add(listener);
	}

	public void stopListening(String keyspace, ReplicatedKvStoreListener listener) {
		CopyOnWriteArraySet<ReplicatedKvStoreListener> set = listeners.get(keyspace);
		if (set != null) {
			set.remove(listener);
		}
	}

	public Try<CompletableFuture<ClusterResponse>> put(String namespace, byte[] key, byte[] value) {
		return put(namespace, key, genClock(), value);
	}

	// TODO: Map response
	public Try<CompletableFuture<ClusterResponse>> put(String namespace, byte[] key, long clock, byte[] value) {
		return kvStore(namespace).set(key, clock, value).map(opt -> {
			onUpdate(namespace, key, value);
			return cluster.sendRequest(new Set(namespace.getBytes(), key, clock, value).encode(), 5, TimeUnit.SECONDS);
		});
	}

	public Try<CompletableFuture<ClusterResponse>> delete(String namespace, byte[] key) {
		return delete(namespace, key, genClock());
	}

	// TODO: Map delete response
	public Try<CompletableFuture<ClusterResponse>> delete(String namespace, byte[] key, long clock) {
		return kvStore(namespace).delete(key, clock).map(opt -> {
			onDelete(namespace, key);
			return cluster.sendRequest(new Delete(namespace.getBytes(), key, clock).encode(), 5, TimeUnit.SECONDS);
		});
	}

	public Try<byte[]> get(String namespace, byte[] key) {
		return kvStore(namespace).get(key);
	}

	public Stream<ClockedKeyValue> entries(String namespace) {
		return kvStore(namespace).entries();
	}
	
	public <K, V> ReplicatedKeySpace.Builder<K, V> serdeKeySpace(String keyspace, Serde<K> keySerde,
	    Serde<V> valueSerde) {
		return new ReplicatedKeySpace.Builder<K, V>(this, keyspace, keySerde, valueSerde);
	}

	/**
	 * Start the service. This loads the stores from disk, then starts the cluster.
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		loadKeySpaces();
		if (keySpaces.isEmpty()) {
			emptyStore.set(true);
		}
		cluster.start();
	}

	/**
	 * Shutdown the store and cluster.
	 * 
	 * @throws Exception
	 */
	public void stop() throws Exception {
		cluster.stop();
		for (MonotonicKvStore store : keySpaces.values()) {
			store.close();
		}
		keySpaceMeta.close();
		peerCheckins.close();
	}

	///////////////////
	// Non public
	//////////////////

	// This will be a SET, DEL, STATE, or SYNC_REQ for a given namespace
	protected byte[] onPeerRequest(Peer peer, byte[] request) {
		ByteBuffer buffer = ByteBuffer.wrap(request);
		byte code = buffer.get();
		switch (code) {
		case REQ_SET:
			return onPeerSet(peer, new Set(buffer));
		case REQ_DEL:
			return onPeerDelete(peer, new Delete(buffer));
		case REQ_SYNC:
			return onPeerSync(peer, new SyncRequest(buffer));
		case REQ_STATE:
		}
		return AckResponse.error("Unknown request").encode();
	}

	// This is invoked on BATCH after a SYNC_REQ was given
	protected void onPeerSend(Peer peer, byte[] request) {
		ByteBuffer buffer = ByteBuffer.wrap(request);
		byte code = buffer.get();

		if (code != ReplicatedKvProto.SND_BULK) {
			log.error("Unexpected data frame received with code {}", code);
			return;
		}

		SendBulk bulk = new SendBulk(buffer);
		long timestamp = 0;

		log.info("Received sync of {} operations", bulk.ops.size());

		for (KvOp op : bulk.ops) {
			if (op instanceof Set) {
				Set set = (Set) op;
				// If we set it, compare timestamp, then invoke
				if (kvStore(new String(set.namespace)).set(set.key, set.timestamp, set.value).isSuccess()) {
					timestamp = Math.max(timestamp, set.timestamp);
				}
			} else if (op instanceof Delete) {
				Delete delete = (Delete) op;
				if (kvStore(new String(delete.namespace)).delete(delete.key, delete.timestamp).isSuccess()) {
					timestamp = Math.max(timestamp, delete.timestamp);
				}
			}
		}

		if (timestamp > 0) {
			updateLastPeerChange(peer, timestamp);
		}
	}

	protected void onPeerJoin(Peer peer) {
		// If we have no data, let's just request it from peers until we have some
		if (emptyStore.get()) {
			requestSync(peer, 0l);
			return;
		}
		peerCheckins.get(peer.getAddress().toString()).tap(timestamp -> {
			requestSync(peer, timestamp);
		});
	}

	////

	private byte[] onPeerSync(Peer peer, SyncRequest syncRequest) {
		if (emptyStore.get()) {
			return AckResponse.error("New Node").encode();
		}
		cluster.schedule(() -> {
			sendUpdatesSince(syncRequest.timestamp, peer);
		}, 5, TimeUnit.MILLISECONDS);
		return AckResponse.success().encode();
	}

	private void sendUpdatesSince(long timestamp, Peer peer) {
		keySpaces.forEach((keyspace, store) -> {
			log.debug("Sending {}", keyspace);
			List<KvOp> sets = store.entriesAfter(timestamp).map(ckv -> {
				return new Set(keyspace.getBytes(), ckv.getKey(), ckv.getClock(), ckv.getValue());
			}).collect(Collectors.toList());
			peer.send(new SendBulk(sets).encode());
		});
	}

	private byte[] onPeerDelete(Peer peer, Delete delete) {
		Try<byte[]> action = kvStore(new String(delete.namespace)).delete(delete.key, delete.timestamp);
		if (action.isSuccess()) {
			onDelete(new String(delete.namespace), delete.key);
			updateLastPeerChange(peer, delete.timestamp);
			return AckResponse.success().encode();
		}
		return AckResponse.error(action.error().getMessage()).encode();
	}

	private byte[] onPeerSet(Peer peer, Set set) {
		Try<byte[]> action = kvStore(new String(set.namespace)).set(set.key, set.timestamp, set.value);
		if (action.isSuccess()) {
			onUpdate(new String(set.namespace), set.key, set.value);
			updateLastPeerChange(peer, set.timestamp);
			return AckResponse.success().encode();
		}
		return AckResponse.error(action.error().getMessage()).encode();
	}

	private void updateLastPeerChange(Peer peer, long timestamp) {
		peerCheckins.set(peer.getAddress().toString(), timestamp);
		emptyStore.set(false);
	}

	private void requestSync(Peer peer, long sinceTimestamp) {
		log.info("Requesting changes since {} from {}", sinceTimestamp, peer);
		peer.request(new SyncRequest(sinceTimestamp).encode(), (resp) -> {
			// We get an ack, either success (meaning the node is not new and has data), or
			// error, if the node is new and in the same state as us.
			// Once fully sync'd we do a partial fetch
			if (resp.isError()) {
				log.warn("Peer failed to respond to sync request {}: {}", peer, resp.getError().get().getMessage());
				return;
			}

			ByteBuffer buffer = ByteBuffer.wrap(resp.getBytes().get());
			byte code = buffer.get();
			if (code != ReplicatedKvProto.RSP_ACK) {
				log.warn("Unexpected sync response code {} from {}", code, peer);
				return;
			}

			log.info("Sync response Ack from {}", peer);

		}, 1, TimeUnit.SECONDS);
	}

	// TODO: Java9+ nanos?
	private long genClock() {
		return Clock.systemUTC().millis();
	}

	protected Cluster cluster() {
		return cluster;
	}

	private MonotonicKvStore kvStore(String keySpaceName) {
		return keySpaces.computeIfAbsent(keySpaceName, (name) -> {
			try {
				return new MonotonicLogKvStore(Paths.get(storageDir, name));
			} catch (IOException e) {
				return null;
			}
		});
	}

	private void loadKeySpaces() {
		keySpaceMeta.entries().forEach(entry -> kvStore(entry.getKey()));
	}

	private void onDelete(String namespace, byte[] key) {
		CopyOnWriteArraySet<ReplicatedKvStoreListener> set = listeners.get(namespace);
		if (set == null) {
			return;
		}
		for (ReplicatedKvStoreListener listener : set) {
			try {
				listener.onDelete(key);
			} catch (Throwable t) {
				log.error("Failed to invoke delete callback for class {}: {}", listener.getClass(), t.getMessage());
				log.debug("Details", t);
			}
		}
	}

	private void onUpdate(String namespace, byte[] key, byte[] value) {
		CopyOnWriteArraySet<ReplicatedKvStoreListener> set = listeners.get(namespace);
		if (set == null) {
			return;
		}
		for (ReplicatedKvStoreListener listener : set) {
			try {
				listener.onUpdate(key, value);
			} catch (Throwable t) {
				log.error("Failed to invoke delete callback for class {}: {}", listener.getClass(), t.getMessage());
				log.debug("Details", t);
			}
		}
	}
}
