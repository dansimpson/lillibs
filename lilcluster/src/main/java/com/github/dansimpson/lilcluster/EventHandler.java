package com.github.dansimpson.lilcluster;

import java.nio.ByteBuffer;

public interface EventHandler {

	public void onPeerJoin(Peer peer);

	public void onPeerLeave(Peer peer);

	// Is it a broadcast message?
	public void onMessageReceived(Peer peer, ByteBuffer data);

	public ByteBuffer onRequestReceived(Peer peer, ByteBuffer data) throws Exception;

}
