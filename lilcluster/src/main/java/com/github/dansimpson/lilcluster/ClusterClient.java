package com.github.dansimpson.lilcluster;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dansimpson.lilcluster.Peer.ConnectionState;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ClusterClient {

	private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

	// private final Cluster cluster;
	private final InetSocketAddress address;
	private final Bootstrap bootstrap;
	private ChannelFutureListener reconnector;
	private ChannelFuture future;
	private Channel channel;

	public ClusterClient(Cluster cluster, InetSocketAddress address) {
		// this.cluster = cluster;
		this.address = address;

		bootstrap = new Bootstrap().group(cluster.getGroup())
		    .channel(NioSocketChannel.class)
		    .handler(new ClusterChannelInitializer(cluster));
		bootstrap.option(ChannelOption.SO_REUSEADDR, true);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);
		bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		bootstrap.remoteAddress(address);

		reconnector = new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (shouldReconnect(future)) {
					log.debug("Disconnected from {} - Reconnecting in {}ms", address, 1000);
					future.channel().eventLoop().schedule(() -> {
						connect();
					}, 1000, TimeUnit.MILLISECONDS);
				}
			}

			private boolean shouldReconnect(ChannelFuture future) {
				return getChannelHandler().peer.getState() == ConnectionState.DISCONNECTED;
			}
		};
	}

	public boolean isConnected() {
		return channel != null && channel.isActive();
	}

	public boolean isConnecting() {
		return future != null && !future.isDone();
	}

	public void connect() {
		if (isConnected() || isConnecting()) {
			return;
		}

		log.trace("Connecting to {}", address);

		future = bootstrap.connect().addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				channel = future.channel();
				if (future.isSuccess()) {
					channel.closeFuture().addListener(reconnector);
				} else {
					log.info("Failed to connect to remote host {} with reason: {}. Retrying in 1s", address,
					    future.cause().getMessage());

					// TODO: Backoff
					bootstrap.config().group().schedule(() -> {
						connect();
					}, 1, TimeUnit.SECONDS);
				}
			}

		});
	}

	private ClusterChannelHandler getChannelHandler() {
		return channel.pipeline().get(ClusterChannelHandler.class);
	}

}
