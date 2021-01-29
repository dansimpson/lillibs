package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.proto.msg.Attribute;
import com.github.dansimpson.lilcluster.proto.msg.Node;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * A cluster of nodes. Create a cluster using the Cluster.Builder class for configuration. The cluster starts a server, and connects to
 * peers provided by the seeds list. When new peers join, their seeds are taken to expand the cluster. The current topology is star, where
 * each peer to peer link is maintained as a single socket connection.
 * 
 * The cluster is designed to provide relatively simple mechanisms for pub sub and targeted sends. User callbacks are defined in the builder
 * to handle events and user data sends and requests.
 * 
 * @author Dan Simpson
 *
 */
public class Cluster {

	private static final Logger log = LoggerFactory.getLogger(Cluster.class);

	private final InetSocketAddress address;
	private final InetSocketAddress advertisedAddress;
	private final EventLoopGroup group;
	private final Set<InetSocketAddress> seeds;
	private final ClusterServer server;
	private final Node localNode;

	private final Consumer<Peer> onPeerJoinCallback;
	private final Consumer<Peer> onPeerLeaveCallback;
	private final BiFunction<Peer, byte[], byte[]> onPeerRequestFn;
	private final BiConsumer<Peer, byte[]> onPeerSendCallback;
	private final Map<InetSocketAddress, Peer> peers = new HashMap<>();

	protected Cluster(InetSocketAddress address, InetSocketAddress advertisedAddress, EventLoopGroup group,
	    Set<InetSocketAddress> seeds, Node localNode, Consumer<Peer> onPeerJoinCallback,
	    Consumer<Peer> onPeerLeaveCallback, BiFunction<Peer, byte[], byte[]> onPeerRequestFn,
	    BiConsumer<Peer, byte[]> onPeerSendCallback) {
		super();
		this.address = address;
		this.advertisedAddress = advertisedAddress;
		this.group = group;
		this.seeds = seeds;
		this.localNode = localNode;
		this.onPeerJoinCallback = onPeerJoinCallback;
		this.onPeerLeaveCallback = onPeerLeaveCallback;
		this.onPeerRequestFn = onPeerRequestFn;
		this.onPeerSendCallback = onPeerSendCallback;
		this.server = new ClusterServer(this, address);
	}

	protected EventLoopGroup getGroup() {
		return group;
	}

	public void schedule(Runnable command, long delay, TimeUnit unit) {
		group.schedule(command, delay, unit);
	}

	/**
	 * 
	 * @return The bind address for the socket
	 */
	public InetSocketAddress getAddress() {
		return address;
	}

	/**
	 * 
	 * @return The advertised address which other peers use for identification and connecting.
	 */
	public InetSocketAddress getAdvertisedAddress() {
		return advertisedAddress;
	}

	/**
	 * 
	 * @return The list of seed address as provided.
	 */
	public Set<InetSocketAddress> getSeeds() {
		return seeds;
	}

	/**
	 * 
	 * @return All peers which are actively connected
	 */
	public Stream<Peer> getActivePeers() {
		return peers.values().stream().filter(Peer::isActive);
	}

	/**
	 * 
	 * @param key
	 *          - the attribute key
	 * @param value
	 *          - the attribute value to match
	 * @return Stream of active peers with a matching attribute
	 */
	public Stream<Peer> getActivePeersByAttribute(String key, String value) {
		return getActivePeers().filter(p -> {
			Optional<String> check = p.getAttribute(key);
			return check.isPresent() && value.equals(check.get());
		});
	}

	/**
	 * 
	 * @return All peer host addresses which are actively connected
	 */
	public Set<InetSocketAddress> getActivePeerAddresses() {
		return getActivePeers().map(p -> p.getAddress()).collect(Collectors.toSet());
	}

	/////////////////////
	// Scopes
	/////////////////////

	public PeerScope.Builder scoped() {
		return new PeerScope.Builder(this);
	}

	/////////////////////
	// Sends and Requests
	/////////////////////

	/**
	 * Broadcast a binary payload to all active peers
	 * 
	 * @param data
	 *          - Application data, to be received by other nodes, which will invoke the onPeerSend callback
	 */
	public void sendData(byte[] data) {
		sendData(data, getActivePeers());
	}

	/**
	 * Send data to a set of peers.
	 * 
	 * @param data
	 *          - Application data, to be received by other nodes, which will invoke the onPeerSend callback
	 * @param peers
	 *          - The stream of peers to send to.
	 */
	public void sendData(byte[] data, Stream<Peer> peers) {
		peers.forEach(p -> p.send(data));
	}

	/**
	 * Send a request to all active peers, and receive a response within the timeout.
	 * 
	 * @param data
	 *          - The request data
	 * @param callback
	 *          - The callback which is invoked on success, failure, or timeout
	 * @param timeout
	 *          - The number of units to wait for the response
	 * @param unit
	 *          - The time unit for the timeout
	 */
	public void sendRequest(byte[] data, Consumer<ClusterResponse> callback, long timeout, TimeUnit unit) {
		sendRequest(data, callback, getActivePeers(), timeout, unit);
	}

	/**
	 * Send a request to all active peers, and receive a Future response within the timeout.
	 * 
	 * @param data
	 *          - The request data
	 * @param timeout
	 *          - The number of units to wait for the response
	 * @param unit
	 *          - The time unit for the timeout
	 */
	public CompletableFuture<ClusterResponse> sendRequest(byte[] data, long timeout, TimeUnit unit) {
		return sendRequest(data, getActivePeers(), timeout, unit);
	}

	/**
	 * Send a request to a subset of active peers, and receive a response within the timeout.
	 * 
	 * @param data
	 *          - The request data
	 * @param callback
	 *          - The callback which is invoked on success, failure, or timeout
	 * @param peerGroup
	 *          - The peers to request data from
	 * @param timeout
	 *          - The number of units to wait for the response
	 * @param unit
	 *          - The time unit for the timeout
	 */
	public void sendRequest(byte[] data, Consumer<ClusterResponse> callback, Stream<Peer> peerGroup, long timeout,
	    TimeUnit unit) {
		final List<PeerResponse> responses = new CopyOnWriteArrayList<>();
		final List<Peer> peers = peerGroup.collect(Collectors.toList());
		Consumer<PeerResponse> consumer = (response) -> {
			responses.add(response);
			if (responses.size() == peers.size()) {
				callback.accept(new ClusterResponse(responses));
			}
		};
		peers.forEach(p -> p.request(data, consumer, timeout, unit));
	}

	/**
	 * Send a request to a subset of active peers, and receive a Future response within the timeout.
	 * 
	 * @param data
	 *          - The request data
	 * @param peerGroup
	 *          - The peers to request data from
	 * @param timeout
	 *          - The number of units to wait for the response
	 * @param unit
	 *          - The time unit for the timeout
	 */
	public CompletableFuture<ClusterResponse> sendRequest(byte[] data, Stream<Peer> peerGroup, long timeout,
	    TimeUnit unit) {
		CompletableFuture<ClusterResponse> future = new CompletableFuture<>();
		sendRequest(data, (response) -> {
			future.complete(response);
		}, peerGroup, timeout, unit);
		return future;
	}

	/**
	 * Start the server, and connect to seeded peers
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		server.listen();
		connectToPeersWithJitter(50);
	}

	/**
	 * Stop the server, notifying each peer that we are leaving before hand
	 * 
	 * @throws Exception
	 */
	public void stop() throws Exception {
		synchronized (peers) {
			peers.forEach((addr, peer) -> {
				peer.leave("Shutting down");
			});
			peers.clear();
		}
		server.stop();
	}

	/////////////////////////

	protected void onSendReceived(Peer peer, byte[] data) {
		try {
			onPeerSendCallback.accept(peer, data);
		} catch (Throwable err) {
			log.error("Failed to execute onPeerSendCallback: {}", err.getMessage());
		}
	}

	protected byte[] handlePeerRequest(Peer peer, byte[] data) {
		return onPeerRequestFn.apply(peer, data);
	}

	protected void leave(Peer peer) {
		if (peer.wasEstablished() && onPeerLeaveCallback != null) {
			onPeerLeaveCallback.accept(peer);
		}
	}

	protected Node getLocalNode() {
		return localNode;
	}

	protected void connectToPeersWithJitter(long maxJitter) {
		seeds.stream().filter(this::isRemote).forEach((addr) -> {
			synchronized (peers) {
				if (!peers.containsKey(addr)) {
					group.schedule(() -> {
						if (!peers.containsKey(addr)) {
							new ClusterClient(this, addr).connect();
						}
					}, (long) Math.ceil(Math.random() * maxJitter), TimeUnit.MILLISECONDS);
				}
			}
		});
	}

	protected void onPeerGossip(Set<InetSocketAddress> addresses) {
		boolean changed = false;
		for (InetSocketAddress address : addresses) {
			synchronized (seeds) {
				changed |= seeds.add(address);
			}
		}
		if (changed) {
			connectToPeersWithJitter(100);
		}
	}

	protected void onEstablished(Peer peer) {
		if (onPeerJoinCallback == null) {
			return;
		}

		if (peer != peers.get(peer.getAddress())) {
			log.warn("onEstablished called for a peer which isn't the primary!");
		}

		try {
			onPeerJoinCallback.accept(peer);
		} catch (Throwable err) {
			log.error("Failed to execute onPeerJoinCallback: {}", err.getMessage());
		}
	}

	protected boolean join(Peer peer) {
		synchronized (peers) {
			Peer existing = peers.get(peer.getAddress());
			if (existing != null) {
				if (existing == peer) {
					return true;
				}

				// If we prefer this new peer
				if (existing.hasPriorityOver(peer)) {
					return false;
				}
			}

			peers.put(peer.getAddress(), peer);
			if (existing != null) {
				log.debug("Replacing inactive peer {}", peer.getAddress());
				existing.leave("Connection replaced");
			}

			return true;
		}
	}

	protected boolean isLocal(InetSocketAddress address) {
		return address.equals(this.address) || address.equals(this.advertisedAddress);
	}

	private boolean isRemote(InetSocketAddress address) {
		return !isLocal(address);
	}

	public static class Builder {

		private InetSocketAddress address;
		private InetSocketAddress advertisedAddress;
		private EventLoopGroup group = new NioEventLoopGroup();
		private Set<InetSocketAddress> seeds = new HashSet<InetSocketAddress>();

		private Consumer<Peer> onPeerJoinCallback;
		private Consumer<Peer> onPeerLeaveCallback;
		private BiFunction<Peer, byte[], byte[]> onPeerRequestFn;
		private BiConsumer<Peer, byte[]> onPeerSendCallback;

		private List<Attribute> attributes = new ArrayList<>();

		public Builder() {
		}

		public Builder addSeedHosts(Set<InetSocketAddress> seeds) {
			this.seeds.addAll(seeds);
			return this;
		}

		public Builder addSeedHost(String host, int port) {
			this.seeds.add(new InetSocketAddress(host, port));
			return this;
		}

		public Builder setEventLoopGroup(EventLoopGroup group) {
			this.group = group;
			return this;
		}

		public Builder setBindHost(String host, int port) {
			return setBindHost(new InetSocketAddress(host, port));
		}

		public Builder setBindHost(InetSocketAddress address) {
			this.address = address;
			if (this.advertisedAddress == null) {
				this.advertisedAddress = address;
			}
			return this;
		}

		public Builder setAdvertisedHost(String host, int port) {
			this.advertisedAddress = new InetSocketAddress(host, port);
			return this;
		}

		public void onPeerJoin(Consumer<Peer> onPeerJoinCallback) {
			this.onPeerJoinCallback = onPeerJoinCallback;
		}

		public void onPeerLeave(Consumer<Peer> onPeerLeaveCallback) {
			this.onPeerLeaveCallback = onPeerLeaveCallback;
		}

		public void onPeerRequest(BiFunction<Peer, byte[], byte[]> onPeerRequestFn) {
			this.onPeerRequestFn = onPeerRequestFn;
		}

		public void onPeerSend(BiConsumer<Peer, byte[]> onPeerSendCallback) {
			this.onPeerSendCallback = onPeerSendCallback;
		}

		public Cluster build() {
			Node node = new Node(advertisedAddress.getHostString(), advertisedAddress.getPort(), attributes);
			return new Cluster(address, advertisedAddress, group, seeds, node, onPeerJoinCallback, onPeerLeaveCallback,
			    onPeerRequestFn, onPeerSendCallback);
		}

		public Builder setAttribute(String key, String value) {
			attributes.add(new Attribute(key, value));
			return this;
		}
	}

}
