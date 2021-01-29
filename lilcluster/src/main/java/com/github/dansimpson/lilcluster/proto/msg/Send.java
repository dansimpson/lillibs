package com.github.dansimpson.lilcluster.proto.msg;

public class Send implements ClusterMessage {

	private final byte[] data;

	public Send(byte[] data) {
		super();
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.Send;
	}

}
