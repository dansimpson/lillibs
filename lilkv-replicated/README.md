## lilkv-replicated

![artifact](https://github.com/dansimpson/lilkv-replicated/workflows/packages/badge.svg)
![test](https://github.com/dansimpson/lilkv-replicated/workflows/tests/badge.svg)

A replicated key value store with key spaces.

Goals:
 * Embeddable
 * Lightweight
 * Ideal for small data sets (fits in RAM)
 * Persistent / Crash Tolerant
 * Eventually consistent with repair

### Usage

```java
Cluster.Builder builder = new Cluster.Builder()
    .setBindHost("0.0.0.0", 10000)
    .setAdvertisedHost("192.168.0.50", 10000)
    .addSeedHost("192.168.0.60", 10000)
    .addSeedHost("192.168.0.70", 10000);

ReplicateKvStore store = new ReplicatedKvStore("/var/data/kv", builder);
store.start();
store.stop();

// ReplicatedKvStore must work with keyspaces, either a standard
// keyspace with byte[] keys and values, or a serde enhanced keySpace
ReplicatedKeySpace weapons = store.keySpace("weapons");
weapons.onPut((key, value) -> {
  // handle key, value byte[]
});
weapons.onDelete((key) -> {
  // handle key byte[]
});

weapons.delete("key".getBytes());

```

### Building

```
gradle clean test assemble
```
