package com.github.dansimpson.lilcluster.proto.msg;

public class Response implements ClusterMessage {

	private final byte[] data;
	private final long id;

	public Response(byte[] data, long id) {
		super();
		this.data = data;
		this.id = id;
	}

	public byte[] getData() {
		return data;
	}

	public long getId() {
		return id;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.Response;
	}

}
