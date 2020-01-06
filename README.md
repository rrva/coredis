# Coredis

A kotlin redis client based on non-blocking I/O with coroutines.
This is just some experimental code I wrote when learning about coroutines
and Channels, not very well tested.

## Usage

Create a client instance:

```kotlin
val redis = RedisClient(InetSocketAddress("localhost", 6379))
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

## Supported redis commands

- `setex` set a key with expire time in seconds
- `del(key)`: delete a key
- `ping(echomsg)` - ping the redis server, accepts optional text to echo back
- `ttl(key)` - show the ttl of a key

## Configuration

The RedisClient class supports the following constructor parameters:

- `address` Redis server address
- `commandTimeoutMillis` timeout for sending commands
- `replyTimeoutMillis` timeout for waiting to read a reply
- `dbIndex` select another redis database for all connections other than 0
- `maxPoolSize` Maximum number of connections to open

## TODO

- Needs more test coverage. This is alpha quality, has not been used in production yet.
- Redesign the error handling and possibly add more timeouts
- Better reconnect and pooling logic
- Support setting and getting byte arrays in addition to utf-8 strings
- Implement many more redis commands
