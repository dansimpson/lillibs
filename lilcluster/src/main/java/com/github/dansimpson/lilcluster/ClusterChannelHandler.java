package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.Peer.ConnectionState;
import com.github.dansimpson.lilcluster.proto.msg.Announce;
import com.github.dansimpson.lilcluster.proto.msg.AnnounceAck;
import com.github.dansimpson.lilcluster.proto.msg.ClusterMessage;
import com.github.dansimpson.lilcluster.proto.msg.Established;
import com.github.dansimpson.lilcluster.proto.msg.Leave;
import com.github.dansimpson.lilcluster.proto.msg.Request;
import com.github.dansimpson.lilcluster.proto.msg.Response;
import com.github.dansimpson.lilcluster.proto.msg.Send;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * The last stage in the channel pipeline. All decoded messages are handled here, and dispatched to the appropriate receivers. Some messages
 * are part of the cluster establishment protocol, and others are for handling the library consumers data. This is the handler for both the
 * client and the server.
 * 
 * @author Dan Simpson
 *
 */
public class ClusterChannelHandler extends SimpleChannelInboundHandler<ClusterMessage> {

	private static final Logger log = LoggerFactory.getLogger(ClusterChannelHandler.class);

	private final Cluster cluster;
	protected Peer peer;
	protected ChannelHandlerContext context;

	public ClusterChannelHandler(Cluster cluster) {
		this.cluster = cluster;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ClusterMessage msg) throws Exception {
		switch (msg.getMessageType()) {
		case Announce:
			onAnnounce((Announce) msg);
			break;
		case AnnounceAck:
			onAnnounceAck((AnnounceAck) msg);
			break;
		case Established:
			onEstablished();
			break;
		case Leave:
			onLeave((Leave) msg);
			break;
		case Request:
			onRequest((Request) msg);
			break;
		case Response:
			onResponse((Response) msg);
			break;
		case Send:
			onSend((Send) msg);
			break;
		default:
			log.warn("Unexpected Message Received: {}", msg.toString());
			break;
		}
	}

	private void onResponse(Response response) {
		peer.onResponse(response);
	}

	private void onRequest(Request request) {
		byte[] data = cluster.handlePeerRequest(peer, request.getData());
		context.writeAndFlush(new Response(data, request.getId()));
	}

	private void onSend(Send send) {
		cluster.onSendReceived(peer, send.getData());
	}

	private void onEstablished() {
		peer.updateState(ConnectionState.ESTABLISHED);
		log.info("Connection Established: {}", peer);
		cluster.onEstablished(peer);
	}

	private void onLeave(Leave leave) {
		peer.updateState(ConnectionState.CLOSING);
		context.close();
	}

	private void onAnnounce(Announce announce) {
		log.debug("Received announce from: {}:{}", announce.getNode().getHost(), announce.getNode().getPort());
		peer.setNode(announce.getNode());
		peer.setTimestamp(announce.getTimestamp());
		peer.setUUID(announce.getUuid());
		if (cluster.join(peer)) {
			peer.updateState(ConnectionState.HANDSHAKING);
			send(new AnnounceAck(cluster.getLocalNode()));
		} else {
			peer.leave("Peer already registed on another connection");
		}
	}

	private void onAnnounceAck(AnnounceAck ack) {
		peer.setNode(ack.getNode());
		if (cluster.join(peer)) {
			sendEstablished().addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					onEstablished();
				}
			});
		} else {
			peer.leave("Peer already registed on another connection");
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		context = ctx;
		peer = new Peer(this);
		// If this node is the originator of the connection, start the handshake
		if (cluster.isLocal((InetSocketAddress) ctx.channel().localAddress())) {
			peer.setTimestamp(System.currentTimeMillis());
			peer.setUUID(UUID.randomUUID());
			send(new Announce(cluster.getLocalNode(), peer.getTimestamp(), peer.getUUID()));
		}
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		peer.onClose();
		cluster.leave(peer);
		super.channelInactive(ctx);
	}

	public ChannelFuture send(ClusterMessage message) {
		return context.writeAndFlush(message);
	}

	public void sendData(byte[] data) {
		send(new Send(data));
	}

	public void sendRequest(byte[] data, long requestId) {
		send(new Request(data, requestId));
	}

	protected void leave(String reason) {
		send(new Leave(reason)).addListener(ChannelFutureListener.CLOSE);
	}

	protected ChannelFuture sendEstablished() {
		return context.writeAndFlush(new Established());
	}

}
