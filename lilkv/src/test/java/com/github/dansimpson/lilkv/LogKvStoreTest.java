package com.github.dansimpson.lilkv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogKvStoreTest {

  private static final Path dir = Paths.get("/tmp/kv-store");
  private LogKvStore store;

  private byte[] key1 = "key".getBytes();
  private byte[] val1 = "value".getBytes();

  public void reload() throws IOException {
    store.close();
    store = new LogKvStore(dir, 100, 25);
  }

  @Before
  public void setup() throws Exception {
    store = new LogKvStore(dir, 100, 25);
    Assert.assertTrue(store.getDbFilePath().toFile().exists());
  }

  @After
  public void teardown() throws IOException {
    store.close();
    Files.walk(dir)
        .map(Path::toFile)
        .forEach(
            (file) -> {
              if (!file.isDirectory()) {
                file.delete();
              }
            });
  }

  @Test
  public void testSet() {
    store.set(key1, val1);
    Assert.assertTrue(store.get(key1).isSuccess());
  }

  @Test
  public void testDelete() {
    store.set(key1, val1);
    Assert.assertTrue(store.delete(key1).isSuccess());
    Assert.assertFalse(store.get(key1).isSuccess());
  }

  @Test
  public void testDeleteReopen() throws Exception {
    store.set(key1, val1);
    Assert.assertTrue(store.delete(key1).isSuccess());
    reload();
    Assert.assertFalse(store.get(key1).isSuccess());
  }

  @Test
  public void testPersistence() throws IOException {
    store.set(key1, val1);
    byte[] data = Files.readAllBytes(store.getDbFilePath());
    Assert.assertEquals(20, data.length);
  }

  @Test
  public void testDedup() throws IOException {
    store.set(key1, val1);
    store.set(key1, val1);
    store.set(key1, val1);
    store.set(key1, val1);
    byte[] data = Files.readAllBytes(store.getDbFilePath());
    Assert.assertEquals(20, data.length);
  }

  @Test
  public void testReload() throws IOException {
    store.set(key1, val1);
    reload();
    Assert.assertArrayEquals("value".getBytes(), store.get(key1).get());
  }

  @Test
  public void testAppend() throws IOException {
    store.set(key1, val1);
    reload();
    store.set("yek".getBytes(), val1);
    Assert.assertEquals(40, Files.readAllBytes(store.getDbFilePath()).length);
  }

  @Test
  public void testCompact() throws IOException {
    for (int i = 0; i < 101; i++) {
      store.set(key1, String.valueOf(i).getBytes());
    }
    Assert.assertEquals(1708, Files.readAllBytes(store.getDbFilePath()).length);
    store.set(key1, val1);
    Assert.assertEquals(18, Files.readAllBytes(store.getDbFilePath()).length);
  }

  @Test(timeout = 5000)
  public void testSpeed() throws IOException {
    byte[] data = new byte[2048];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    int count = 100000;
    long t1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      store.set(("key" + i).getBytes(), data);
    }
    long t2 = System.nanoTime();

    Assert.assertEquals(count, store.entries().count());

    reload();
    long t3 = System.nanoTime();

    Assert.assertEquals(count, store.entries().count());

    System.out.printf("Bench Set (%d ops): Time: %dms\n", count, (t2 - t1) / 1000000);
    System.out.printf("Bench Load (%d keys): Time: %dms\n", count, (t3 - t2) / 1000000);
  }

  @Test(timeout = 5000)
  public void testDedupSpeed() throws IOException {
    byte[] data = new byte[2048];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    int count = 100000;
    long t1 = System.nanoTime();
    for (int i = 0; i < count; i++) {
      store.set(key1, data);
    }
    long t2 = System.nanoTime();

    System.out.printf("Bench Set (%d ops): Time w/ Dedups: %dms\n", count, (t2 - t1) / 1000000);
  }
}
