package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A reusable scope for a peer group. This can be used to target a subset of peers for sends, requests, etc. This only works with ACTIVE
 * peers.
 * 
 * @author Dan Simpson
 *
 */
public class PeerScope {

	private final Cluster cluster;
	private final Predicate<Peer> filter;
	private final long defaultTimeoutMillis;

	protected PeerScope(Cluster cluster, List<Predicate<Peer>> filters, long defaultTimeoutMillis) {
		this.cluster = cluster;
		this.filter = filters.stream().reduce(Predicate::and).get();
		this.defaultTimeoutMillis = defaultTimeoutMillis;
	}

	public void sendData(byte[] data) {
		cluster.sendData(data, peers());
	}

	public CompletableFuture<ClusterResponse> sendRequest(byte[] data) {
		return sendRequest(data, defaultTimeoutMillis, TimeUnit.MILLISECONDS);
	}

	public CompletableFuture<ClusterResponse> sendRequest(byte[] data, long timeout, TimeUnit unit) {
		return cluster.sendRequest(data, peers(), timeout, unit);
	}

	public Stream<Peer> peers() {
		return cluster.getActivePeers().filter(filter);
	}

	protected static class Builder {

		private Cluster cluster;
		private List<Predicate<Peer>> filters = new ArrayList<>();
		private long timeoutMillis = TimeUnit.SECONDS.toMillis(30);

		protected Builder(Cluster cluster) {
			this.cluster = cluster;
		}

		public Builder filterAttribute(String key) {
			filters.add((peer) -> peer.hasAttribute(key));
			return this;
		}

		public Builder filterAttribute(String key, String value) {
			filters.add((peer) -> peer.getAttribute(key).map(v -> value.equals(v)).orElse(false));
			return this;
		}

		public Builder filterAddresses(InetSocketAddress... addresses) {
			final Set<InetSocketAddress> set = new HashSet<>(Arrays.asList(addresses));
			filters.add((peer) -> set.contains(peer.getAddress()));
			return this;
		}

		public Builder withDefaultTimeout(long timeout, TimeUnit unit) {
			this.timeoutMillis = unit.toMillis(timeout);
			return this;
		}

		public PeerScope build() {
			return new PeerScope(cluster, filters, timeoutMillis);
		}
	}

}
