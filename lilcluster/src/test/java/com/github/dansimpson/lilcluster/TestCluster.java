package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestCluster {

	private final Cluster cluster;

	public List<Peer> joins = new CopyOnWriteArrayList<>();
	public List<Peer> leaves = new CopyOnWriteArrayList<>();
	public List<byte[]> messages = new CopyOnWriteArrayList<>();
	public List<byte[]> requests = new CopyOnWriteArrayList<>();

	public AtomicInteger activePeers = new AtomicInteger(0);

	public TestCluster(int port, int[] peerPorts) {
		super();
		Cluster.Builder builder = new Cluster.Builder().setBindHost("localhost", port)
		    .setAttribute("port", String.valueOf(port))
		    .addSeedHosts(
		        Arrays.stream(peerPorts).mapToObj(p -> new InetSocketAddress("localhost", p)).collect(Collectors.toSet()));

		builder.onPeerJoin((peer) -> {
			activePeers.incrementAndGet();
			joins.add(peer);
		});

		builder.onPeerLeave((peer) -> {
			activePeers.decrementAndGet();
			leaves.add(peer);
		});

		builder.onPeerRequest((peer, data) -> {
			requests.add(data);
			return data;
		});

		builder.onPeerSend((peer, data) -> {
			messages.add(data);
		});

		cluster = builder.build();
	}

	public Cluster cluster() {
		return cluster;
	}

	public void start() throws Exception {
		cluster.start();
	}

	public void stop() throws Exception {
		cluster.stop();
	}

	public void awaitForPeers(int count) throws InterruptedException {
		while (activePeers.get() < count) {
			Thread.sleep(5);
		}
	}

}
