package com.github.dansimpson.lilcluster;

import com.github.dansimpson.lilcluster.proto.codec.ClusterMessageDecoder;
import com.github.dansimpson.lilcluster.proto.codec.ClusterMessageEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;

/**
 * A standard pipeline initialize which works for both the client and server.
 * 
 * @author Dan Simpson
 *
 */
public class ClusterChannelInitializer extends ChannelInitializer<SocketChannel> {

	private final Cluster cluster;

	public ClusterChannelInitializer(Cluster cluster) {
		this.cluster = cluster;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new SnappyFrameEncoder());
		pipeline.addLast(new SnappyFrameDecoder());
		pipeline.addLast(new ClusterMessageDecoder());
		pipeline.addLast(new ClusterMessageEncoder());
		pipeline.addLast(new ClusterChannelHandler(cluster));
	}

}
