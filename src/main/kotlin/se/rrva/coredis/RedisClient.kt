package se.rrva.coredis

import io.ktor.network.selector.ActorSelectorManager
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.util.KtorExperimentalAPI
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import se.rrva.coredis.connection.*
import se.rrva.coredis.connection.readRedisResponses
import se.rrva.coredis.connection.sendRedisCommands

/**
 * A kotlin redis client based on non-blocking I/O with coroutines
 *
 * @param address Redis server address
 * @param commandTimeoutMillis timeout for sending commands
 * @param replyTimeoutMillis timeout for waiting to read a reply
 * @param dbIndex select another redis database for all connections other than 0
 * @param maxPoolSize Maximum number of connections to open
 */
class RedisClient(
    private val address: SocketAddress = InetSocketAddress("127.0.0.1", 6379),
    private val commandTimeoutMillis: Long = 15000L,
    private val replyTimeoutMillis: Long = 15000L,
    private val dbIndex: Int = 0,
    private val maxPoolSize: Int = 50
) : CoroutineScope, Redis {

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + Job()

    private val log = LoggerFactory.getLogger(this::class.java)

    private val nonBusyConnections = AtomicInteger()
    private val connectionCount = AtomicInteger()

    private val sharedCommandChannel = Channel<RedisCommand>()

    @kotlinx.coroutines.ExperimentalCoroutinesApi
    @io.ktor.util.KtorExperimentalAPI
    private fun CoroutineScope.startRedisConnection(id: Int, commandChannel: Channel<RedisCommand>) = launch {
        nonBusyConnections.incrementAndGet()
        val socket = aSocket(ActorSelectorManager(Dispatchers.IO)).tcp().connect(address)
        val writeChannel = socket.openWriteChannel(autoFlush = true)
        log.info("$id connected to redis at ${socket.remoteAddress}")
        val sentCommands = sendRedisCommands(
            writeChannel,
            commandChannel,
            nonBusyConnections,
            dbIndex,
            commandTimeoutMillis
        )
        val readChannel = socket.openReadChannel()
        val readRedisResponsesJob = readRedisResponses(RedisReader(readChannel), sentCommands, replyTimeoutMillis)
        readRedisResponsesJob.join()
        log.debug("$id closing")
        nonBusyConnections.decrementAndGet()
        connectionCount.decrementAndGet()
        socket.close()
    }

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun send(cmd: String, vararg args: String): String {
        return sendNullable(cmd, *args) ?: throw IllegalStateException("$cmd should never have a null response")
    }

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun sendNullable(cmd: String, vararg args: String): String? {
        val arrayArgs = args.map { it.toByteArray() }.toTypedArray()
        return sendNullable(cmd, *arrayArgs)
    }

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun sendNullable(cmd: String, vararg args: ByteArray): String? {
        val nonBusy = nonBusyConnections.get()
        if (nonBusy == 0 && connectionCount.get() < maxPoolSize) {
            log.debug(
                "Found $nonBusy non-busy connections" +
                        ", and current active count is ${connectionCount.get()}, opening a new connection"
            )
            startRedisConnection(connectionCount.incrementAndGet(), sharedCommandChannel)
        } else {
            log.debug("Reusing existing redis connection")
        }
        val response = Channel<RedisCommandResponse>()
        sharedCommandChannel.send(RedisCommand(cmd, args.toList(), response))
        try {
            return withTimeout(2 * (commandTimeoutMillis + replyTimeoutMillis)) {
                when (val result = response.receive()) {
                    is RedisCommandResponse.Data -> result.data
                    is RedisCommandResponse.Error -> throw RedisException(result.error, result.cause)
                }
            }
        } catch (c: TimeoutCancellationException) {
            throw RedisException("$cmd failed with ${c.message ?: "unknown error"}", c.cause)
        }
    }

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun ping(vararg echoText: String): String = send("ping", *echoText)

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun set(key: String, value: String): String = send("set", key, value)

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun setex(key: String, value: String, expireSeconds: Long): String =
        send("setex", key, "$expireSeconds", value)

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun get(key: String): String? = sendNullable("get", key.toByteArray())

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun del(key: String): String = send("del", key)

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    override suspend fun ttl(key: String): String = send("ttl", key)
}
