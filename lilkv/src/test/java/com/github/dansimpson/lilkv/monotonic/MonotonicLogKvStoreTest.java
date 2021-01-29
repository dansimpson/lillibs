package com.github.dansimpson.lilkv.monotonic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.dansimpson.lilkv.KvStore.StaleClockError;
import com.github.dansimpson.lilkv.KvStore.ValueUnchangedError;

public class MonotonicLogKvStoreTest {

	private static final Path dir = Paths.get("/tmp/kv-store");
	private MonotonicLogKvStore store;

	private byte[] key1 = "key".getBytes();
	private byte[] val1 = "value".getBytes();
	private long clock = 10000;

	public void reload() throws IOException {
		store.close();
		store = new MonotonicLogKvStore(dir, 100, 25);
	}

	@Before
	public void setup() throws Exception {
		store = new MonotonicLogKvStore(dir, 100, 25);
		Assert.assertTrue(store.getDbFilePath().toFile().exists());
	}

	@After
	public void teardown() throws IOException {
		store.close();
		Files.walk(dir).map(Path::toFile).forEach((file) -> {
			if (!file.isDirectory()) {
				file.delete();
			}
		});
	}

	@Test
	public void testSet() {
		store.set(key1, clock, val1);
		Assert.assertTrue(store.get(key1).isSuccess());
	}

	@Test
	public void testDelete() {
		store.set(key1, clock, val1);
		Assert.assertNotNull(store.delete(key1, clock + 1));
		Assert.assertFalse(store.get(key1).isSuccess());
	}

	@Test
	public void testDeleteStale() {
		store.set(key1, clock, val1);
		Assert.assertTrue(store.delete(key1, clock).isFailure());
		Assert.assertTrue(store.delete(key1, clock).error() instanceof StaleClockError);
		Assert.assertTrue(store.get(key1).isSuccess());
	}

	@Test
	public void testPersistence() throws IOException {
		store.set(key1, clock, val1);
		byte[] data = Files.readAllBytes(store.getDbFilePath());
		Assert.assertEquals(28, data.length);
	}

	@Test
	public void testReload() throws IOException {
		store.set(key1, clock, val1);
		reload();
		Assert.assertArrayEquals(val1, store.get(key1).get());
		Assert.assertEquals(clock, store.getClock(key1).get().longValue());
	}

	@Test
	public void testDeleteReopen() throws Exception {
		store.set(key1, clock, val1);
		Assert.assertNotNull(store.delete(key1, clock + 1));
		reload();
		Assert.assertFalse(store.get(key1).isSuccess());
		Assert.assertFalse(store.getClock(key1).isSuccess());
	}

	@Test
	public void testDedup() throws IOException {
		store.set(key1, clock, val1);
		Assert.assertTrue(store.set(key1, clock + 1, val1).isFailure());
		Assert.assertTrue(store.set(key1, clock + 1, val1).error() instanceof ValueUnchangedError);
		byte[] data = Files.readAllBytes(store.getDbFilePath());
		Assert.assertEquals(28, data.length);
	}

	@Test
	public void testDedupClock() throws IOException {
		store.set(key1, clock, val1);
		store.set(key1, clock, "other".getBytes());
		byte[] data = Files.readAllBytes(store.getDbFilePath());
		Assert.assertEquals(28, data.length);
	}

	@Test
	public void testAppend() throws IOException {
		store.set(key1, clock, val1);
		reload();
		store.set("yek".getBytes(), clock, val1);
		Assert.assertEquals(56, Files.readAllBytes(store.getDbFilePath()).length);
	}

	@Test
	public void testCompact() throws IOException {
		for (int i = 0; i < 101; i++) {
			store.set(key1, clock + i, String.valueOf(i).getBytes());
		}
		Assert.assertEquals(2516, Files.readAllBytes(store.getDbFilePath()).length);
		store.set(key1, clock + 102, val1);
		Assert.assertEquals(26, Files.readAllBytes(store.getDbFilePath()).length);
	}

	@Test
	public void testStream() throws IOException {
		store.set(key1, clock, val1);
		store.set("key2".getBytes(), clock, val1);
		Assert.assertEquals(2, store.entries().count());
	}

	@Test
	public void testStreamRangeBetween() throws IOException {
		store.set("1".getBytes(), 1000, val1);
		store.set("2".getBytes(), 1001, val1);
		store.set("3".getBytes(), 1002, val1);
		Assert.assertEquals(3, store.entriesBetween(1000, 1002, true, true).count());
		Assert.assertEquals(1, store.entriesBetween(1000, 1002).count());
		Assert.assertArrayEquals("2".getBytes(), store.entriesBetween(1000, 1002).findFirst().get().getKey());
	}

	@Test
	public void testStreamRangeAfter() throws IOException {
		store.set("1".getBytes(), 1000, val1);
		store.set("2".getBytes(), 1001, val1);
		store.set("3".getBytes(), 1002, val1);
		Assert.assertEquals(3, store.entriesAfter(1000, true).count());
		Assert.assertEquals(2, store.entriesAfter(1000).count());
	}

}
