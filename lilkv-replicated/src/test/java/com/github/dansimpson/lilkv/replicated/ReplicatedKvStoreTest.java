package com.github.dansimpson.lilkv.replicated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.dansimpson.lilcluster.ClusterResponse;
import com.github.dansimpson.lilkv.KvStore.ValueUnchangedError;
import com.github.dansimpson.lilkv.util.Try;

public class ReplicatedKvStoreTest {

	private TestReplicatedKvStore store1;
	private TestReplicatedKvStore store2;
	private TestReplicatedKvStore store3;

	@BeforeClass
	public static void config() {
		// System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
	}

	@Before
	public void setup() throws Exception {
		store1 = new TestReplicatedKvStore(10001, new int[] { 10001, 10002, 10003 });
		store2 = new TestReplicatedKvStore(10002, new int[] { 10001, 10002, 10003 });
		store3 = new TestReplicatedKvStore(10003, new int[] { 10001, 10002, 10003 });
		store1.start();
		store2.start();
		store3.start();
	}

	@After
	public void teardown() throws Exception {
		store1.stop();
		store2.stop();
		store3.stop();
		Files.walk(Paths.get(TestReplicatedKvStore.DIR)).map(Path::toFile).forEach((file) -> {
			if (!file.isDirectory()) {
				file.delete();
			}
		});
	}

	@Test(timeout = 2000)
	public void testBasic() throws Exception {
		store1.awaitForPeers(2);
		store2.awaitForPeers(2);
		store3.awaitForPeers(2);

		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		Try<CompletableFuture<ClusterResponse>> result = store1.store().put("keyspace", key, value);

		Assert.assertTrue(result.isSuccess());
		Assert.assertFalse(result.get().get().getResponses().isEmpty());

		Assert.assertArrayEquals(value, store1.store().get("keyspace", key).get());
		Assert.assertArrayEquals(value, store2.store().get("keyspace", key).get());
		Assert.assertArrayEquals(value, store3.store().get("keyspace", key).get());

		// Should be unchanged exception
		result = store1.store().put("keyspace", key, value);

		Assert.assertTrue(result.isFailure());
		Assert.assertTrue(result.error() instanceof ValueUnchangedError);
	}

	@Test(timeout = 5000)
	public void testRepair() throws Exception {
		store1.awaitForPeers(2);
		store2.awaitForPeers(2);
		store3.awaitForPeers(2);

		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		Try<CompletableFuture<ClusterResponse>> result = store1.store().put("keyspace", key, value);

		Assert.assertTrue(result.isSuccess());
		Assert.assertFalse(result.get().get().getResponses().isEmpty());

		store3.stop();
		store1.store().put("keyspace", "key1".getBytes(), value);
		store1.store().put("keyspace", "key2".getBytes(), value).get().get();

		Assert.assertArrayEquals(value, store2.store().get("keyspace", key).get());

		// Not found
		Assert.assertFalse(store3.store().get("keyspace", "key1".getBytes()).isSuccess());

		store3 = new TestReplicatedKvStore(10003, new int[] { 10001, 10002, 10003 });
		store3.start();
		store3.awaitForPeers(2);

		while (store3.store().get("keyspace", "key1".getBytes()).isFailure()) {
			Thread.sleep(5);
		}

		Assert.assertArrayEquals(value, store3.store().get("keyspace", key).get());
	}

	@Test(timeout = 5000)
	public void testNewNodeSync() throws Exception {
		store1.awaitForPeers(2);
		store2.awaitForPeers(2);
		store3.awaitForPeers(2);

		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		Try<CompletableFuture<ClusterResponse>> result = store1.store().put("keyspace", key, value);

		Assert.assertTrue(result.isSuccess());
		Assert.assertFalse(result.get().get().getResponses().isEmpty());
		store1.store().put("keyspace", "key1".getBytes(), value);
		store1.store().put("keyspace", "key2".getBytes(), value).get().get();

		Assert.assertArrayEquals(value, store2.store().get("keyspace", key).get());
		TestReplicatedKvStore store4 = new TestReplicatedKvStore(10004, new int[] { 10001, 10002, 10003 });
		store4.start();
		store4.awaitForPeers(3);

		while (store4.store().get("keyspace", "key1".getBytes()).isFailure()) {
			Thread.sleep(5);
		}

		Assert.assertArrayEquals(value, store4.store().get("keyspace", key).get());
	}

	@Test(timeout = 5000)
	public void testListeners() throws Exception {
		store1.awaitForPeers(2);
		store2.awaitForPeers(2);
		store3.awaitForPeers(2);

		final AtomicInteger updateCount = new AtomicInteger();
		final AtomicInteger deleteCount = new AtomicInteger();
		ReplicatedKvStoreListener listener = new ReplicatedKvStoreListener() {

			@Override
			public void onUpdate(byte[] key, byte[] value) {
				updateCount.incrementAndGet();
			}

			@Override
			public void onDelete(byte[] key) {
				deleteCount.incrementAndGet();
			}

		};

		store1.store().listen("keyspace", listener);
		store2.store().listen("keyspace", listener);
		store3.store().listen("keyspace", listener);

		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		Assert.assertEquals(2, store1.store().put("keyspace", key, 1l, value).get().get().getResponses().size());
		Assert.assertEquals(2, store1.store().delete("keyspace", key, 2l).get().get().getResponses().size());
		// Check count of updates

		Assert.assertEquals(3, updateCount.get());
		Assert.assertEquals(3, deleteCount.get());

		store1.store().stopListening("keyspace", listener);
		store2.store().stopListening("keyspace", listener);

		Assert.assertEquals(2, store1.store().put("keyspace", key, 3l, value).get().get().getResponses().size());

		Assert.assertEquals(4, updateCount.get());
		Assert.assertEquals(3, deleteCount.get());
	}

}
