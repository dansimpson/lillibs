package com.github.dansimpson.lilcluster;

import java.nio.ByteBuffer;
import java.util.Optional;

public class PeerResponse {

	private final Peer peer;
	private final Optional<byte[]> buffer;
	private final Optional<Throwable> error;

	public PeerResponse(Peer peer, byte[] buffer) {
		this(peer, Optional.of(buffer), Optional.empty());
	}

	public PeerResponse(Peer peer, Throwable error) {
		this(peer, Optional.empty(), Optional.of(error));
	}

	public PeerResponse(Peer peer, Optional<byte[]> buffer, Optional<Throwable> error) {
		super();
		this.peer = peer;
		this.buffer = buffer;
		this.error = error;
	}

	public boolean isError() {
		return error.isPresent();
	}

	public boolean isSuccess() {
		return buffer.isPresent();
	}

	public Peer getPeer() {
		return peer;
	}

	public Optional<byte[]> getBytes() {
		return buffer;
	}

	public Optional<ByteBuffer> getByteBuffer() {
		return getBytes().map(buf -> {
			return ByteBuffer.wrap(buf);
		});
	}

	public Optional<String> getBufferString() {
		return getBytes().map(String::new);
	}

	public Optional<Throwable> getError() {
		return error;
	}

	public void throwIfError() throws Throwable {
		if (isError()) {
			throw error.get();
		}
	}

}
