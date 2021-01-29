package com.github.dansimpson.lilkv.replicated;

public interface ReplicatedKvStoreListener {

	public void onUpdate(byte[] key, byte[] value);

	public void onDelete(byte[] key);
}
