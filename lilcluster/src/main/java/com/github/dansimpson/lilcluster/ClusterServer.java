package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class ClusterServer {

	private static final Logger log = LoggerFactory.getLogger(ClusterServer.class);

	private final InetSocketAddress address;
	private final ServerBootstrap bootstrap;

	public ClusterServer(Cluster cluster, InetSocketAddress address) {
		this.address = address;

		bootstrap = new ServerBootstrap();
		bootstrap.option(ChannelOption.SO_REUSEADDR, true);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
		bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		bootstrap.group(cluster.getGroup())
		    .channel(NioServerSocketChannel.class)
		    .childHandler(new ClusterChannelInitializer(cluster));
	}

	public void listen() throws Exception {
		bootstrap.bind(address.getPort()).sync();
		log.info("Listening on port {}", address.getPort());
	}

	public void stop() throws Exception {
		log.info("Shutting down");
		bootstrap.config().group().shutdownGracefully();
		bootstrap.config().childGroup().shutdownGracefully();
	}

}
