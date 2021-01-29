package com.github.dansimpson.lilkv.serde;

import java.nio.ByteBuffer;

public class LongSerde implements Serde<Long> {

	@Override
	public byte[] serialize(Long object) throws SerdeException {
		return ByteBuffer.allocate(8).putLong(object).array();
	}

	@Override
	public Long deserialize(byte[] bytes) throws SerdeException {
		return ByteBuffer.wrap(bytes).getLong();
	}

}
