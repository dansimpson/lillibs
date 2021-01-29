package com.github.dansimpson.lilcluster.proto.msg;

public class Established implements ClusterMessage {

	@Override
	public MessageType getMessageType() {
		return MessageType.Established;
	}

}
