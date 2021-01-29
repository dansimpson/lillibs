package com.github.dansimpson.lilcluster.proto.msg;

public class Leave implements ClusterMessage {

	private final String reason;

	public Leave(String reason) {
		super();
		this.reason = reason;
	}

	public String getReason() {
		return reason;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.Leave;
	}

}