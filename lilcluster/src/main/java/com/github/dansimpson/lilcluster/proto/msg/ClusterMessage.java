package com.github.dansimpson.lilcluster.proto.msg;

public interface ClusterMessage {

	public MessageType getMessageType();

	public default byte getMessageCode() {
		return getMessageType().code;
	}

}
