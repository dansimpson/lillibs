## lilcluster

![artifact](https://github.com/dansimpson/lilcluster/workflows/packages/badge.svg)
![test](https://github.com/dansimpson/lilcluster/workflows/tests/badge.svg)

Small library for clustering nodes.

Active Goals
 * Few dependencies (slf4j-api, netty-handler)
 * Seeds + Gossip for membership
 * Support blind sends and receives
 * Support request/response pattern (single peer and multiple)
 * User data is just byte arrays
 * Stable API

Wishlist
 * Support for various toplogies
 * Election and leadership utilities
 * Abstractions ofr oplogs with vector clocks
 * Traffic Priority (Queues) 

### Usage

```java
Cluster.Builder builder = new Cluster.Builder()
    .setBindHost("0.0.0.0", 10000) // The interface and port to bind to
    .setAdvertisedHost("192.168.0.50", 10000) // The gossip host (for peers to connect to)
    .setAttribute("group", "frontend") // Attributes are used to filter peers for messaging
    .setAttribute("zone", "us-east-1a")
    .addSeedHost("192.168.0.60", 10000) // Seed nodes are nodes which are used to relay peer info at boot 
    .addSeedHost("192.168.0.70", 10000);

builder.onPeerJoin((peer) -> {
  // A peer has joined / reconnected
});

builder.onPeerLeave((peer) -> {
  // A peer has left, disconnected, see peer.getState(), peer.getMessage()
});

builder.onPeerRequest((peer, data) -> {
  // A peer has requested something from this node, respond with a byte[]
  return data;
});

builder.onPeerSend((peer, data) -> {
  // A peer has sent data without an expected response
});


// Build the cluster instance and start it!
Cluster cluster = cluster.build();
cluster.start();
cluster.stop();
```

### Sending and Requesting Data

```java
byte[] data = "hello".getBytes();

// Send a message to all active peers
cluster.sendData(data);

// Request data from all peers
CompletableFuture<List<PeerResponse>> responses = cluster.sendRequest(data, 1l, TimeUnit.SECONDS);
responses.get().forEach((response) -> {
  Peer peer = response.getPeer();
  response.getError().ifPresent(e -> log.error("Request failure {}: {}", peer, e.getMessage()));
  response.getBytes().ifPresent(b -> processResponse(peer, b));
});
```

#### Reusable Scopes

Scopes allow you to target a specific group of nodes based on attributes or addresses.

```java
PeerScope cacheGroup = cluster
  .scoped()
  .filterAttribute("role", "cache")
  .withDefaultTimeout(10, TimeUnit.SECONDS)
  .build();

// Send requests to group and get a future
CompletableFuture<List<PeerResponse>> future = cacheGroup.sendRequest(data);
future.get().forEach(response -> {});

// Async Without a future
cacheGroup.sendRequest(data, (responses) -> {
  responses.forEach(response -> {});
});

// Send to all without an expected response
cacheGroup.sendData(data);

// Fetch active peers with applied scope filters
cacheGroup.peers().forEach((peer) -> {});

// Send an item to the first peer
cacheGroup.peers().findFirst().ifPresent(peer -> {
  peer.send(data);
});
```

### Building

```
gradle clean test assemble
```
