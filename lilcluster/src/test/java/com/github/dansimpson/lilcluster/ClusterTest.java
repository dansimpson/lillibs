package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.Peer.RequestTimeout;

public class ClusterTest {

	private static final String PING_STR = "ping";
	private static final byte[] PING = PING_STR.getBytes();

	private static final Logger log = LoggerFactory.getLogger(ClusterTest.class);
	private TestCluster cluster1;
	private TestCluster cluster2;
	private TestCluster cluster3;

	@BeforeClass
	public static void config() {
		// System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
	}

	@Before
	public void setup() throws Exception {
		cluster1 = new TestCluster(10001, new int[] { 10001, 10002, 10003 });
		cluster2 = new TestCluster(10002, new int[] { 10001, 10002, 10003 });
		cluster3 = new TestCluster(10003, new int[] { 10001, 10002, 10003 });
		cluster1.start();
		cluster2.start();
		cluster3.start();
	}

	@After
	public void teardown() throws Exception {
		cluster1.stop();
		cluster2.stop();
		cluster3.stop();
	}

	@Test(timeout = 2000)
	public void testClustering() throws Exception {
		cluster1.awaitForPeers(2);
		cluster2.awaitForPeers(2);
		cluster3.awaitForPeers(2);

		Assert.assertEquals(2, cluster1.cluster().getActivePeerAddresses().size());
		Assert.assertEquals(2, cluster2.cluster().getActivePeerAddresses().size());
		Assert.assertEquals(2, cluster3.cluster().getActivePeerAddresses().size());

		Assert.assertThat(cluster1.cluster().getActivePeerAddresses(),
		    CoreMatchers.hasItems(new InetSocketAddress("localhost", 10003), new InetSocketAddress("localhost", 10002)));
		Assert.assertThat(cluster2.cluster().getActivePeerAddresses(),
		    CoreMatchers.hasItems(new InetSocketAddress("localhost", 10003), new InetSocketAddress("localhost", 10001)));
		Assert.assertThat(cluster3.cluster().getActivePeerAddresses(),
		    CoreMatchers.hasItems(new InetSocketAddress("localhost", 10002), new InetSocketAddress("localhost", 10001)));

	}

	@Test(timeout = 5000)
	public void testBroadcast() throws Exception {
		cluster1.awaitForPeers(2);
		cluster2.cluster().sendData(PING);
		while (cluster1.messages.size() < 1) {
			Thread.sleep(1);
		}

		Assert.assertArrayEquals(PING, cluster1.messages.get(0));
	}

	@Test(timeout = 5000)
	public void testRequestSync() throws Exception {
		cluster1.awaitForPeers(2);

		Optional<Peer> peer = cluster1.cluster().getActivePeersByAttribute("port", "10002").findFirst();
		Assert.assertTrue(peer.isPresent());

		PeerResponse item = peer.get().requestSync(PING, 1, TimeUnit.SECONDS);
		Assert.assertEquals(PING_STR, item.getBufferString().get());
	}

	@Test(timeout = 5000)
	public void testRequestFuture() throws Exception {
		cluster1.awaitForPeers(2);

		Optional<Peer> peer = cluster1.cluster().getActivePeersByAttribute("port", "10002").findFirst();
		Assert.assertTrue(peer.isPresent());

		CompletableFuture<PeerResponse> item = peer.get().request(PING, 1, TimeUnit.SECONDS);
		Assert.assertEquals(PING_STR, item.get().getBufferString().get());
	}

	@Test(timeout = 5000)
	public void testRequestCallback() throws Exception {
		cluster1.awaitForPeers(2);

		Optional<Peer> peer = cluster1.cluster().getActivePeersByAttribute("port", "10002").findFirst();
		Assert.assertTrue(peer.isPresent());

		LinkedBlockingQueue<PeerResponse> queue = new LinkedBlockingQueue<>();

		peer.get().request(PING, (r) -> queue.add(r), 1, TimeUnit.SECONDS);
		PeerResponse response = queue.take();
		Assert.assertEquals(PING_STR, response.getBufferString().get());
	}

	@Test(timeout = 5000)
	public void testRequestMulti() throws Exception {
		cluster1.awaitForPeers(2);

		CompletableFuture<ClusterResponse> item = cluster1.cluster().sendRequest(PING, 1l, TimeUnit.SECONDS);
		item.get().getResponses().forEach((r) -> {
			Assert.assertTrue(r.isSuccess());
			Assert.assertEquals(PING_STR, r.getBufferString().get());
		});
	}

	@Test(timeout = 5000)
	public void testScopeSend() throws Exception {
		cluster1.awaitForPeers(2);

		PeerScope scope = cluster1.cluster()
		    .scoped()
		    .filterAttribute("port", "10002")
		    .withDefaultTimeout(100, TimeUnit.MILLISECONDS)
		    .build();

		scope.sendData(PING);

		while (cluster2.messages.size() < 1) {
			Thread.sleep(1);
		}

		Assert.assertArrayEquals(PING, cluster2.messages.get(0));
	}

	@Test(timeout = 2000)
	public void testTimeout() throws Exception {
		cluster1.awaitForPeers(2);

		PeerScope scope = cluster1.cluster()
		    .scoped()
		    .filterAttribute("port", "10002")
		    .withDefaultTimeout(0, TimeUnit.MILLISECONDS)
		    .build();

		PeerResponse item = scope.sendRequest(PING).get().getResponses().get(0);
		Assert.assertTrue(item.isError());
		Assert.assertTrue(item.getError().get() instanceof RequestTimeout);
	}

}
