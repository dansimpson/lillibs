package com.github.dansimpson.lilcluster.proto.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.github.dansimpson.lilcluster.proto.msg.Announce;
import com.github.dansimpson.lilcluster.proto.msg.AnnounceAck;
import com.github.dansimpson.lilcluster.proto.msg.Attribute;
import com.github.dansimpson.lilcluster.proto.msg.Established;
import com.github.dansimpson.lilcluster.proto.msg.Leave;
import com.github.dansimpson.lilcluster.proto.msg.MessageType;
import com.github.dansimpson.lilcluster.proto.msg.Node;
import com.github.dansimpson.lilcluster.proto.msg.Request;
import com.github.dansimpson.lilcluster.proto.msg.Response;
import com.github.dansimpson.lilcluster.proto.msg.Send;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class ClusterMessageDecoder extends ByteToMessageDecoder {

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

		// read the length
		// if we have enough, decode it
		if (in.readableBytes() < 6) {
			return;
		}

		in.markReaderIndex();

		byte version = in.readByte();
		byte code = in.readByte();
		int length = in.readInt();

		if (in.readableBytes() < length) {
			in.resetReaderIndex();
			return;
		}

		out.add(decode(code, length, in));
	}

	private Object decode(byte code, int length, ByteBuf in) {
		switch (MessageType.forCode(code)) {
		case Announce:
			return decodeAnnounce(in);
		case AnnounceAck:
			return decodeAnnounceAck(in);
		case Established:
			return new Established();
		case Invalid:
			return null;
		case Leave:
			return decodeLeave(in);
		case Request:
			return decodeRequest(in);
		case Response:
			return decodeResponse(in);
		case Send:
			return decodeSend(in);
		}

		// TODO Auto-generated method stub
		return null;
	}

	private Send decodeSend(ByteBuf in) {
		byte[] data = decodeBytes(in);
		return new Send(data);
	}

	private Response decodeResponse(ByteBuf in) {
		long id = in.readLong();
		byte[] data = decodeBytes(in);
		return new Response(data, id);
	}

	private Request decodeRequest(ByteBuf in) {
		long id = in.readLong();
		byte[] data = decodeBytes(in);
		return new Request(data, id);
	}

	private Leave decodeLeave(ByteBuf in) {
		String reason = decodeString(in);
		return new Leave(reason);
	}

	private AnnounceAck decodeAnnounceAck(ByteBuf in) {
		Node node = decodeNode(in);
		return new AnnounceAck(node);
	}

	private Announce decodeAnnounce(ByteBuf in) {
		Node node = decodeNode(in);
		long timestamp = in.readLong();
		String uuid = decodeString(in);
		return new Announce(node, timestamp, UUID.fromString(uuid));
	}

	private Node decodeNode(ByteBuf in) {
		String host = decodeString(in);
		int port = in.readInt();
		List<Attribute> attributes = new ArrayList<>();
		short attrCount = in.readShort();
		for (short i = 0; i < attrCount; i++) {
			attributes.add(decodeAttribute(in));
		}
		return new Node(host, port, attributes);
	}

	private Attribute decodeAttribute(ByteBuf in) {
		String key = decodeString(in);
		String value = decodeString(in);
		return new Attribute(key, value);
	}

	private byte[] decodeBytes(ByteBuf in) {
		byte[] data = new byte[in.readInt()];
		in.readBytes(data);
		return data;
	}

	private String decodeString(ByteBuf in) {
		return new String(decodeBytes(in));
	}

}