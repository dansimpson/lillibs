package com.github.dansimpson.lilkv.serde;

public interface Serde<T> {

	public static class SerdeException extends Exception {

		private static final long serialVersionUID = -8390359732215244892L;

		public SerdeException() {
			super();
		}

		public SerdeException(Throwable cause) {
			super(cause);
		}
	}

	public byte[] serialize(T object) throws SerdeException;

	public T deserialize(byte[] bytes) throws SerdeException;
}
