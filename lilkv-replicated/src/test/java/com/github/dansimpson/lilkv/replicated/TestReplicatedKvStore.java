package com.github.dansimpson.lilkv.replicated;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.github.dansimpson.lilcluster.Cluster;

public class TestReplicatedKvStore {

	public static final String DIR = "/tmp/rkv";

	private ReplicatedKvStore store;

	public TestReplicatedKvStore(int port, int[] peerPorts) throws IOException {
		super();
		Cluster.Builder builder = new Cluster.Builder().setBindHost("localhost", port)
		    .setAttribute("port", String.valueOf(port))
		    .addSeedHosts(
		        Arrays.stream(peerPorts).mapToObj(p -> new InetSocketAddress("localhost", p)).collect(Collectors.toSet()));

		store = new ReplicatedKvStore(DIR + "/node" + port, builder);
	}

	public ReplicatedKvStore store() {
		return store;
	}

	public void start() throws Exception {
		store.start();
	}

	public void stop() throws Exception {
		store.stop();
	}

	public void awaitForPeers(int count) throws InterruptedException {
		while (store.cluster().getActivePeers().count() < count) {
			Thread.sleep(5);
		}
	}

}
