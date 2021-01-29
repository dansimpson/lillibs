package com.github.dansimpson.lilkv.serde;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.dansimpson.lilkv.LogKvStore;

public class SerdeKVStoreTest {

	private static final Path dir = Paths.get("/tmp/serde-store");
	private SerdeKvStore<String, Long> store;

	@Before
	public void setup() throws Exception {
		store = new SerdeKvStore<>(new LogKvStore(dir, 100, 25), new StringSerde(), new LongSerde());
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
	public void testGetAndSet() {
		Assert.assertTrue(store.get("hello").isFailure());
		Assert.assertTrue(store.set("hello", 15l).isSuccess());
		Assert.assertTrue(store.get("hello").isSuccess());
		Assert.assertEquals(15l, store.get("hello").get().longValue());
	}

}
