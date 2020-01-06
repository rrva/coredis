# Coredis

A kotlin redis client based on non-blocking I/O with coroutines.
This is just some experimental code I wrote when learning about coroutines
and Channels, not very well tested.

## Usage

Create a client instance:

```kotlin
val redis = Redis(InetSocketAddress("localhost", 6379))
```

The instance manages its own connection pool, auto-scales 
upto default 50 connections. It uses redis command pipelining
to possibly send multiple commands on the same connection 
before a reply has been received.

Set a key, (a supending function). Encodes strings as UTF-8.

```kotlin
redis.set("somekey", "somevalue")
```

Get a key, decodes data as UTF-8

```kotlin
val stringValue = redis.get("somekey")
```

# Supported redis commands

`setex` set a key with expire time in seconds
`del(key)`: delete a key
`ping(echomsg)` - ping the redis server, accepts optional text to echo back
`ttl(key)` - show the ttl of a key

# Configuration

The `Redis` class supports 

TODO

- Needs more tests. Missing tests of the parser, error/timeout handling, concurrency safety tests. This is alpha quality.
- Redesign the error handling and possibly add more timeouts
- Better reconnect and pooling logic
- Support setting and getting byte arrays in addition to utf-8 strings
- Implement many more redis commands