package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.github.dansimpson.lilcluster.proto.msg.Node;
import com.github.dansimpson.lilcluster.proto.msg.Response;

import io.netty.channel.EventLoopGroup;

/**
 * A representation of a Peer in the cluster.
 * 
 * @author Dan Simpson
 *
 */
public class Peer {

	public static class RequestTimeout extends Exception {

		private static final long serialVersionUID = -499269766533780413L;

		public RequestTimeout(long durationMillis) {
			super(String.format("Request timed out after %d milliseconds", durationMillis));
		}
	}

	public static enum ConnectionState {

		CONNECTED(0),
		HANDSHAKING(1),
		ESTABLISHED(2),
		CLOSING(-2),
		DISCONNECTED(-1);

		protected final int priority;

		private ConnectionState(int priority) {
			this.priority = priority;
		}
	}

	private final ClusterChannelHandler context;
	private final EventLoopGroup group;
	private long timestamp;
	private UUID uuid;
	private ConnectionState state;
	private InetSocketAddress address;
	private Map<String, String> attributes = new HashMap<>();
	private final AtomicLong requestIdGenerator = new AtomicLong(1);
	private final Map<Long, Consumer<PeerResponse>> callbacks = new ConcurrentHashMap<>();

	// Toggled when the connection is established to know if the peer was
	// ever in an established state.
	private boolean established = false;

	protected Peer(ClusterChannelHandler context) {
		this(context, ConnectionState.CONNECTED);
	}

	protected Peer(ClusterChannelHandler context, ConnectionState state) {
		this.context = context;
		this.group = context.context.channel().eventLoop();
		this.state = state;
	}

	public ConnectionState getState() {
		return state;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public boolean isActive() {
		return state == ConnectionState.ESTABLISHED;
	}

	public Optional<String> getAttribute(String key) {
		return Optional.ofNullable(attributes.get(key));
	}

	public void send(byte[] data) {
		context.sendData(data);
	}

	public void request(byte[] data, Consumer<PeerResponse> responseHandler, long timeout, TimeUnit unit) {
		final long requestId = requestIdGenerator.incrementAndGet();
		callbacks.put(requestId, responseHandler);

		group.schedule(() -> {
			Consumer<PeerResponse> handler = callbacks.remove(requestId);
			if (handler != null) {
				handler.accept(new PeerResponse(this, new RequestTimeout(unit.toMillis(timeout))));
			}
		}, timeout, unit);

		context.sendRequest(data, requestId);
	}

	public CompletableFuture<PeerResponse> request(byte[] data, long timeout, TimeUnit units) {
		CompletableFuture<PeerResponse> future = new CompletableFuture<>();
		request(data, (response) -> {
			future.complete(response);
		}, timeout, units);
		return future;
	}

	public PeerResponse requestSync(byte[] data, long timeout, TimeUnit units)
	    throws InterruptedException, ExecutionException {
		return request(data, timeout, units).get();
	}

	////

	protected void onResponse(Response response) {
		Consumer<PeerResponse> callback = callbacks.remove(response.getId());
		if (callback == null) {
			return;
		}
		callback.accept(new PeerResponse(this, response.getData()));
	}

	protected void leave(String reason) {
		context.leave(reason);
	}

	protected void setNode(Node node) {
		address = new InetSocketAddress(node.getHost(), node.getPort());
		node.getAttributes().forEach((attr) -> attributes.put(attr.getKey(), attr.getValue()));
	}

	protected void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	protected void updateState(ConnectionState state) {
		this.state = state;
		if (state == ConnectionState.ESTABLISHED) {
			this.established = true;
		}
	}

	// Only update the state to disconnected if the close wasn't expected.
	protected void onClose() {
		if (state != ConnectionState.CLOSING) {
			updateState(ConnectionState.DISCONNECTED);
		}
	}

	protected boolean hasPriorityOver(Peer peer) {
		if (state.priority < 0 && peer.state.priority >= 0) {
			return false;
		} else if (timestamp != peer.timestamp) {
			return timestamp < peer.timestamp;
		}
		return uuid.compareTo(peer.uuid) < 0;
	}

	protected long getTimestamp() {
		return timestamp;
	}

	public void setUUID(UUID uuid) {
		this.uuid = uuid;
	}

	public UUID getUUID() {
		return uuid;
	}

	public String toString() {
		return String.format("%s [%s] (%s)", address, uuid, state);
	}

	public boolean wasEstablished() {
		return established;
	}

	public boolean hasAttribute(String key) {
		return attributes.containsKey(key);
	}

}
