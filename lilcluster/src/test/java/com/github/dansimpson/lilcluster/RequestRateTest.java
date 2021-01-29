package com.github.dansimpson.lilcluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestRateTest {

  private static final Logger log = LoggerFactory.getLogger(ClusterTest.class);
  private static final int nodeCount = 20;
  private List<TestCluster> clusters;

  @Before
  public void setup() throws Exception {
    clusters = new ArrayList<>();
    int[] ports = IntStream.range(10000, 10000 + nodeCount).toArray();
    for (int port : ports) {
      TestCluster cluster = new TestCluster(port, ports);
      clusters.add(cluster);
      cluster.start();
    }
  }

  @After
  public void teardown() throws Exception {
    for (TestCluster cluster : clusters) {
      cluster.stop();
    }
  }

  @Test(timeout = 60000)
  @Ignore
  public void testRequestThroughput() throws Exception {
    for (TestCluster cluster : clusters) {
      cluster.awaitForPeers(nodeCount - 1);
    }

    int count = 100000;
    int executed = 0;
    long t1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      CompletableFuture<ClusterResponse> item =
          clusters.get(0).cluster().sendRequest("hi".getBytes(), 1l, TimeUnit.SECONDS);
      item.get()
          .getResponses()
          .forEach(
              (r) -> {
                Assert.assertTrue(r.isSuccess() && "hi".equals(r.getBufferString().get()));
              });
      executed += item.get().getResponses().size();
    }
    long t2 = System.nanoTime();

    log.info("Executed {} transactions in {}ms", executed, (t2 - t1) / 1000000);
  }

  @Test(timeout = 20000)
  @Ignore
  public void testRequestSingleNode() throws Exception {
    for (TestCluster cluster : clusters) {
      cluster.awaitForPeers(nodeCount - 1);
    }

    Peer peer = clusters.get(0).cluster().getActivePeers().findFirst().get();

    int count = 50000;

    ExecutorService service = Executors.newFixedThreadPool(8);
    CountDownLatch latch = new CountDownLatch(count);

    byte[] data = "hello world".getBytes();

    long t1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      service.execute(
          () -> {
            peer.request(
                data,
                (r) -> {
                  Assert.assertArrayEquals(data, r.getBytes().get());
                  latch.countDown();
                },
                500,
                TimeUnit.SECONDS);
          });
    }

    latch.await(15, TimeUnit.SECONDS);
    long t2 = System.nanoTime();

    log.info("Executed {} transactions in {}ms", count, (t2 - t1) / 1000000);
  }

  @Test(timeout = 20000)
  @Ignore
  public void testRequestCallbackMultiNode() throws Exception {
    for (TestCluster cluster : clusters) {
      cluster.awaitForPeers(nodeCount - 1);
    }
    Cluster cluster = clusters.get(0).cluster();

    int count = 50000;
    AtomicInteger executed = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(8);
    CountDownLatch latch = new CountDownLatch(count);

    byte[] data = "hello world".getBytes();

    long t1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      service.execute(
          () -> {
            cluster.sendRequest(
                data,
                (r) -> {
                  executed.addAndGet(r.getResponses().size());
                  latch.countDown();
                },
                500,
                TimeUnit.SECONDS);
          });
    }

    latch.await(15, TimeUnit.SECONDS);
    long t2 = System.nanoTime();

    log.info("Executed {} transactions in {}ms", executed.get(), (t2 - t1) / 1000000);
  }
}
