package com.github.dansimpson.lilkv.serde;

import org.junit.Assert;
import org.junit.Test;

import com.github.dansimpson.lilkv.serde.Serde.SerdeException;

public class SerdeTest {

	@Test
	public void testStringSerde() throws SerdeException {
		StringSerde serde = new StringSerde();
		String value = "hello world!";
		Assert.assertEquals(value, serde.deserialize(serde.serialize(value)));
	}

	@Test
	public void testLongSerde() throws SerdeException {
		LongSerde serde = new LongSerde();
		Assert.assertEquals(20200201l, serde.deserialize(serde.serialize(20200201l)).longValue());
	}

	@Test
	public void testInstanceSerde() throws SerdeException {
		StringSerde serde = new StringSerde();
		String value = "hello world!";
		Assert.assertEquals(value, serde.deserialize(serde.serialize(value)));
	}
}
