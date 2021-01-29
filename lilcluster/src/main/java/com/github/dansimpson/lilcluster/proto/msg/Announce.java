package com.github.dansimpson.lilcluster.proto.msg;

import java.util.UUID;

public class Announce implements ClusterMessage {

	private final Node node;
	private final long timestamp;
	private final UUID uuid;

	public Announce(Node node, long timestamp, UUID uuid) {
		super();
		this.node = node;
		this.timestamp = timestamp;
		this.uuid = uuid;
	}

	public Node getNode() {
		return node;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public UUID getUuid() {
		return uuid;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.Announce;
	}

}
