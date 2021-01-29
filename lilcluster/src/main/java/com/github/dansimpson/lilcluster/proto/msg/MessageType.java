package com.github.dansimpson.lilcluster.proto.msg;

public enum MessageType {

	Invalid(0, 0),
	Announce(1, 32),
	AnnounceAck(2, 16),
	Established(3, 0),
	Leave(4, 16),
	Send(5, 64),
	Request(6, 64),
	Response(7, 64);

	public final byte code;
	public final int inititialBufferSize;

	private MessageType(int code, int inititialBufferSize) {
		this.code = (byte) code;
		this.inititialBufferSize = inititialBufferSize;
	}

	public static MessageType[] lookup = new MessageType[] { Invalid, Announce, AnnounceAck, Established, Leave, Send,
	    Request, Response };

	public static MessageType forCode(byte code) {
		return lookup[code];
	}

}
