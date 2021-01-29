package com.github.dansimpson.lilkv.replicated;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicatedKvProto {

	protected static final byte REQ_SET = 0x01;
	protected static final byte REQ_DEL = 0x02;
	protected static final byte REQ_SYNC = 0x03;
	protected static final byte REQ_STATE = 0x04;

	protected static final byte RSP_ACK = 0x10;
	protected static final byte RSP_STATE = 0x11;

	protected static final byte SND_BULK = 0x20;

	protected static interface KvOp {

		public byte[] encode();
	}

	protected static class Set implements KvOp {

		protected final long timestamp;
		protected final byte[] namespace;
		protected final byte[] key;
		protected final byte[] value;

		protected Set(ByteBuffer buffer) {
			namespace = new byte[buffer.getShort()];
			buffer.get(namespace);
			key = new byte[buffer.getShort()];
			buffer.get(key);
			value = new byte[buffer.getInt()];
			buffer.get(value);
			timestamp = buffer.getLong();
		}

		public Set(byte[] namespace, byte[] key, long clock, byte[] value) {
			super();
			this.namespace = namespace;
			this.key = key;
			this.value = value;
			this.timestamp = clock;
		}

		public byte[] encode() {
			ByteBuffer buffer = ByteBuffer.allocate(namespace.length + key.length + value.length + 1 + 8 + 8);
			buffer.put(REQ_SET);
			buffer.putShort((short) namespace.length);
			buffer.put(namespace);
			buffer.putShort((short) key.length);
			buffer.put(key);
			buffer.putInt(value.length);
			buffer.put(value);
			buffer.putLong(timestamp);
			return buffer.array();
		}

	}

	protected static class Delete implements KvOp {

		protected final long timestamp;
		protected final byte[] namespace;
		protected final byte[] key;

		protected Delete(ByteBuffer buffer) {
			namespace = new byte[buffer.getShort()];
			buffer.get(namespace);
			key = new byte[buffer.getShort()];
			buffer.get(key);
			timestamp = buffer.getLong();
		}

		public Delete(byte[] namespace, byte[] key, long clock) {
			super();
			this.namespace = namespace;
			this.key = key;
			this.timestamp = clock;
		}

		public byte[] encode() {
			ByteBuffer buffer = ByteBuffer.allocate(namespace.length + key.length + 1 + 4 + 8);
			buffer.put(REQ_DEL);
			buffer.putShort((short) namespace.length);
			buffer.put(namespace);
			buffer.putShort((short) key.length);
			buffer.put(key);
			buffer.putLong(timestamp);
			return buffer.array();
		}

	}

	protected static class AckResponse {

		protected final byte status;
		protected final byte[] error;

		protected AckResponse(ByteBuffer buffer) {
			status = buffer.get();
			if (isError()) {
				error = new byte[buffer.getInt()];
				buffer.get(error);
			} else {
				error = new byte[0];
			}
		}

		private boolean isError() {
			return status > 0;
		}

		public AckResponse(byte status, byte[] error) {
			super();
			this.status = status;
			this.error = error;
		}

		public byte[] encode() {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 1 + (isError() ? (error.length + 4) : 0));
			buffer.put(RSP_ACK);
			buffer.put(status);
			if (isError()) {
				buffer.putInt(error.length);
				buffer.put(error);
			}
			return buffer.array();
		}

		public static AckResponse success() {
			return new AckResponse((byte) 0, new byte[0]);
		}

		public static AckResponse error(String error) {
			return new AckResponse((byte) 1, error == null ? new byte[0] : error.getBytes());
		}
	}

	protected static class SyncRequest {

		protected final long timestamp;

		protected SyncRequest(ByteBuffer buffer) {
			timestamp = buffer.getLong();
		}

		public SyncRequest(long timestamp) {
			super();
			this.timestamp = timestamp;
		}

		public byte[] encode() {
			ByteBuffer buffer = ByteBuffer.allocate(8 + 1);
			buffer.put(REQ_SYNC);
			buffer.putLong(timestamp);
			return buffer.array();
		}

	}

	protected static class SendBulk {

		protected final List<KvOp> ops;

		protected SendBulk(ByteBuffer buffer) {
			ops = new ArrayList<>();
			while (buffer.hasRemaining()) {
				byte code = buffer.get();
				if (code == REQ_SET) {
					ops.add(new Set(buffer));
				} else if (code == REQ_DEL) {
					ops.add(new Delete(buffer));
				}
			}
		}

		public SendBulk(List<KvOp> ops) {
			super();
			this.ops = ops;
		}

		// TODO: Perf/zerocopy
		public byte[] encode() {
			List<byte[]> parts = ops.stream().map(KvOp::encode).collect(Collectors.toList());
			int size = parts.stream().mapToInt(array -> array.length).sum();
			ByteBuffer buffer = ByteBuffer.allocate(size + 1);
			buffer.put(SND_BULK);
			for (byte[] part : parts) {
				buffer.put(part);
			}
			return buffer.array();
		}

	}

}
