package se.rrva.coredis.connection

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.io.ByteReadChannel
import kotlinx.coroutines.io.ByteWriteChannel
import kotlinx.coroutines.io.writePacket
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.io.charsets.CharsetDecoder
import org.slf4j.LoggerFactory
import se.rrva.coredis.protocol.RedisParser
import se.rrva.coredis.protocol.readCRLFLine
import se.rrva.coredis.protocol.writeRedisCommand

private val log = LoggerFactory.getLogger("sendRedisCommands")
private val utf8 = Charsets.UTF_8

@kotlinx.coroutines.ExperimentalCoroutinesApi
internal fun CoroutineScope.sendRedisCommands(
    writeChannel: ByteWriteChannel,
    commandChannel: Channel<RedisCommand>,
    nonBusyConnections: AtomicInteger,
    dbIndex: Int,
    timeoutMillis: Long
) = produce {
    if (dbIndex != 0) {
        selectDb(writeChannel, dbIndex)
    }
    for (cmd in commandChannel) {
        logCommandToSend(cmd)
        if (writeChannel.isClosedForWrite) {
            log.debug("Channel is closed for write, resending command so that another coroutine can pick it up")
            commandChannel.send(cmd)
            break
        }
        nonBusyConnections.decrementAndGet()
        if (sendRedisCommand(writeChannel, cmd, timeoutMillis)) {
            send(cmd)
            nonBusyConnections.incrementAndGet()
        }
    }
    log.debug("closing my producer channel")
    close()
    log.debug("sendRedisCommands done")
}

private suspend fun sendRedisCommand(writeChannel: ByteWriteChannel, cmd: RedisCommand, timeoutMillis: Long): Boolean {
    try {
        withTimeout(timeoutMillis) {
            writeChannel.writePacket {
                writeRedisCommand(cmd.command, cmd.args)
            }
            log.debug("Sent command")
        }
    } catch (te: TimeoutCancellationException) {
        val error = "Failed to send redis command ${cmd.command}, got timeout"
        cmd.response.send(RedisCommandResponse.Error(error))
        return false
    } catch (ioe: IOException) {
        val error = "Failed to send redis command ${cmd.command}, I/O error: ${ioe.message}"
        cmd.response.send(RedisCommandResponse.Error(error, ioe))
        return false
    }

    return true
}

private suspend fun selectDb(writeChannel: ByteWriteChannel, dbIndex: Int) = writeChannel.writePacket {
    writeRedisCommand("select", listOf("$dbIndex".toByteArray()))
}

private fun logCommandToSend(cmd: RedisCommand) =
    log.debug("Picking ${cmd.command} ${cmd.args?.firstOrNull()?.toString(charset("utf8")) ?: ""} to send")

internal fun CoroutineScope.readRedisResponses(
    redisReader: RedisReader,
    sentCommandsWaitingForReply: ReceiveChannel<RedisCommand>,
    replyTimeoutMillis: Long
) = launch {

    val buffer = ByteBuffer.allocate(8192)
    val charBuffer = CharBuffer.allocate(8192)
    val utf8Decoder = utf8.newDecoder()

    loop@ while (redisReader.isOpenForRead()) {
        val commandForReply = sentCommandsWaitingForReply.receiveOrNull()
        when (val reply = redisReader.readNextReply(buffer, charBuffer, utf8Decoder, replyTimeoutMillis)) {
            is RedisReply.Timeout -> {
                commandForReply?.response?.send(
                    RedisCommandResponse.Error(
                        "Failed to read reply for redis command ${commandForReply.command}, got timeout"
                    )
                )
                break@loop
            }
            is RedisReply.Data -> {
                commandForReply?.response?.send(RedisCommandResponse.Data(reply.data))
            }
            is RedisReply.Error -> {
                commandForReply?.response?.send(
                    RedisCommandResponse.Error(
                        reply.message ?: "unknown error",
                        reply.cause
                    )
                )
            }
        }
    }
    log.error("readRedisResponses done")
}

class RedisReader(private val readChannel: ByteReadChannel) {

    suspend fun readNextReply(
        buffer: ByteBuffer,
        charBuffer: CharBuffer,
        utf8Decoder: CharsetDecoder,
        timeoutMillis: Long
    ): RedisReply {
        return try {
            this.readAndParseReply(buffer, charBuffer, utf8Decoder, timeoutMillis)
        } catch (e: TimeoutCancellationException) {
            RedisReply.Timeout
        } catch (e: IOException) {
            utf8Decoder.reset()
            log.error(e.message, e)
            RedisReply.Error(e.message, e)
        }
    }

    private suspend fun readAndParseReply(
        buffer: ByteBuffer,
        charBuffer: CharBuffer,
        utf8Decoder: CharsetDecoder,
        timeoutMillis: Long
    ): RedisReply.Data {
        val redisParser = RedisParser()

        val channel = this
        do {
            val (line, bytesRead) = withTimeout(timeoutMillis) {
                readChannel.readCRLFLine(buffer, charBuffer, utf8Decoder, StringBuilder(), 0)
            }
            redisParser.feed(line, bytesRead)
        } while (redisParser.needsMore())

        return RedisReply.Data(redisParser.message())
    }

    fun isOpenForRead(): Boolean {
        return !readChannel.isClosedForRead
    }
}

sealed class RedisReply {
    object Timeout : RedisReply()
    class Error(val message: String?, val cause: Throwable? = null) : RedisReply()
    class Data(val data: String?) : RedisReply()
}

sealed class RedisCommandResponse {
    class Data(val data: String?) : RedisCommandResponse()
    class Error(val error: String, val cause: Throwable? = null) : RedisCommandResponse()
}

data class RedisCommand(val command: String, val args: List<ByteArray>?, val response: Channel<RedisCommandResponse>)
