package com.github.dansimpson.lilcluster.proto.msg;

import java.util.List;

public class Node {

	private final String host;
	private final int port;
	private final List<Attribute> attributes;

	public Node(String host, int port, List<Attribute> attributes) {
		super();
		this.host = host;
		this.port = port;
		this.attributes = attributes;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

}
