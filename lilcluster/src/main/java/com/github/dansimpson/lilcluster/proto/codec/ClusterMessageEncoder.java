package com.github.dansimpson.lilcluster.proto.codec;

import java.util.List;

import com.github.dansimpson.lilcluster.proto.msg.Announce;
import com.github.dansimpson.lilcluster.proto.msg.AnnounceAck;
import com.github.dansimpson.lilcluster.proto.msg.Attribute;
import com.github.dansimpson.lilcluster.proto.msg.ClusterMessage;
import com.github.dansimpson.lilcluster.proto.msg.Leave;
import com.github.dansimpson.lilcluster.proto.msg.Node;
import com.github.dansimpson.lilcluster.proto.msg.Request;
import com.github.dansimpson.lilcluster.proto.msg.Response;
import com.github.dansimpson.lilcluster.proto.msg.Send;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class ClusterMessageEncoder extends MessageToMessageEncoder<ClusterMessage> {

	@Override
	protected void encode(ChannelHandlerContext ctx, ClusterMessage message, List<Object> out) throws Exception {
		ByteBuf buf = Unpooled.buffer(6 + message.getMessageType().inititialBufferSize);

		// Header (0:byte reserved, 1:byte message code, 2:int32 length)
		buf.writeByte(0);
		buf.writeByte(message.getMessageCode());

		// Set length to 0, then measure written bytes and set it.
		buf.writeInt(0);
		int start = buf.writerIndex();

		// Now write the buffer, and determine
		encode(message, buf);

		// Finally, set the length
		buf.setInt(2, buf.writerIndex() - start);

		out.add(buf);
	}

	private void encode(ClusterMessage message, ByteBuf buf) {
		switch (message.getMessageType()) {
		case Announce:
			encodeAnnounce((Announce) message, buf);
			break;
		case AnnounceAck:
			encodeAnnounceAck((AnnounceAck) message, buf);
			break;
		case Established:
			return;
		case Leave:
			encodeString(((Leave) message).getReason(), buf);
			return;
		case Request:
			encodeRequest((Request) message, buf);
			break;
		case Response:
			encodeResponse((Response) message, buf);
			break;
		case Send:
			encodeSend((Send) message, buf);
			break;
		default:
			break;
		}
	}

	private void encodeBytes(byte[] data, ByteBuf buf) {
		buf.writeInt(data.length);
		buf.writeBytes(data);
	}

	private void encodeString(String str, ByteBuf buf) {
		encodeBytes(str.getBytes(), buf);
	}

	private void encodeNode(Node node, ByteBuf buf) {
		encodeString(node.getHost(), buf);
		buf.writeInt(node.getPort());
		buf.writeShort(node.getAttributes().size());
		for (Attribute attribute : node.getAttributes()) {
			encodeAttribute(attribute, buf);
		}
	}

	private void encodeAttribute(Attribute attribute, ByteBuf buf) {
		encodeString(attribute.getKey(), buf);
		encodeString(attribute.getValue(), buf);
	}

	private void encodeAnnounce(Announce announce, ByteBuf buf) {
		encodeNode(announce.getNode(), buf);
		buf.writeLong(announce.getTimestamp());
		encodeString(announce.getUuid().toString(), buf);
	}

	private void encodeAnnounceAck(AnnounceAck ack, ByteBuf buf) {
		encodeNode(ack.getNode(), buf);
	}

	private void encodeRequest(Request request, ByteBuf buf) {
		buf.writeLong(request.getId());
		encodeBytes(request.getData(), buf);
	}

	private void encodeResponse(Response response, ByteBuf buf) {
		buf.writeLong(response.getId());
		encodeBytes(response.getData(), buf);
	}

	private void encodeSend(Send send, ByteBuf buf) {
		encodeBytes(send.getData(), buf);
	}
}