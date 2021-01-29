## lilkv

![artifact](https://github.com/dansimpson/lilkv/workflows/packages/badge.svg)
![test](https://github.com/dansimpson/lilkv/workflows/tests/badge.svg)

Small embedded KV store that keeps data in a map, and a write ahead log.

Goals:
 * Zero dependencies (Ok, slf4j-api)
 * Memory store backed by disk log
 * Compacting log
 * Quick rebuilds on process restart
 * Serdes for domain objects (keys, values)

### Usage

```java

SerdeKVStore<String, Long> store = new SerdeKVStore<>(
  new LogKvStore("/path/to/dir", 1000, 25), new StringSerde(), new LongSerde()
);

// Try contains Option<Long> or an Exception
Try<Option<Long>> previous = store.set("my key", 15l);
Try<Option<Long>> value = store.get("my key");
Try<Option<Long>> deleted = store.delete("my key");
Try<Stream<String>> keys = store.keys();
```

### Building

```
gradle clean test assemble
```
