package com.github.dansimpson.lilcluster.proto.msg;

public class Attribute {

	private final String key;
	private final String value;

	public Attribute(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

}
