package com.github.dansimpson.lilcluster.proto.msg;

public class AnnounceAck implements ClusterMessage {

	private final Node node;

	public AnnounceAck(Node node) {
		super();
		this.node = node;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.AnnounceAck;
	}

	public Node getNode() {
		return node;
	}

}
