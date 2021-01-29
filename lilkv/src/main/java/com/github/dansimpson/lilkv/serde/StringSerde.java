package com.github.dansimpson.lilkv.serde;

public class StringSerde implements Serde<String> {

	@Override
	public byte[] serialize(String object) throws SerdeException {
		return object.getBytes();
	}

	@Override
	public String deserialize(byte[] object) throws SerdeException {
		return new String(object);
	}

}
