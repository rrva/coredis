package se.rrva.coredis.connection

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.debug.junit4.CoroutinesTimeout
import kotlinx.coroutines.io.ByteWriteChannel
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

class ConnectionHandlerTest {


    @Rule
    @JvmField
    val timeout = CoroutinesTimeout.seconds(10)

    @Test
    fun `sends successfully written from command channel to sent command channel`() {

        runBlocking {
            val writeChannel = mockk<ByteWriteChannel>()
            every { writeChannel.isClosedForWrite } returns false
            coEvery { writeChannel.writePacket(any()) } coAnswers {}

            val commandChannel = Channel<RedisCommand>()
            val nonBusyConnections = AtomicInteger()

            val sentRedisCommands = this.sendRedisCommands(writeChannel, commandChannel, nonBusyConnections, 0, 1000)
            val response = Channel<RedisCommandResponse>()
            commandChannel.send(RedisCommand("ping", emptyList(), response))
            val sentCommand = sentRedisCommands.receive()
            commandChannel.close()

            Assert.assertEquals("ping", sentCommand.command)
        }
    }

    @Test
    fun `successful result sent back to caller`() {

        val writeChannel = mockk<ByteWriteChannel>()
        every { writeChannel.isClosedForWrite } returns false
        coEvery { writeChannel.writePacket(any()) } coAnswers {}

        val redisReader = mockk<RedisReader> {
            coEvery { readNextReply(any(), any(), any(), any()) } coAnswers { RedisReply.Data("PONG") }
            every { isOpenForRead() } returns true andThen false
        }

        runBlocking {

            val commandChannel = Channel<RedisCommand>()
            val nonBusyConnections = AtomicInteger()

            val sentRedisCommands = this.sendRedisCommands(writeChannel, commandChannel, nonBusyConnections, 0, 1000)
            this.readRedisResponses(redisReader, sentRedisCommands, 1000)

            val response = Channel<RedisCommandResponse>()
            commandChannel.send(RedisCommand("ping", emptyList(), response))
            val commandResponse = response.receive() as RedisCommandResponse.Data
            commandChannel.close()
            Assert.assertEquals("PONG", commandResponse.data)
        }

    }

    @Test
    fun `failing writes cause failure to be propagated to caller`() {

        runBlocking {
            val writeChannel = mockk<ByteWriteChannel>()
            every { writeChannel.isClosedForWrite } returns false
            val ioException = IOException("foo")
            coEvery { writeChannel.writePacket(any()) } coAnswers { throw ioException }

            val commandChannel = Channel<RedisCommand>()
            val nonBusyConnections = AtomicInteger()

            this.sendRedisCommands(writeChannel, commandChannel, nonBusyConnections, 0, 1000)
            val response = Channel<RedisCommandResponse>()
            commandChannel.send(RedisCommand("ping", emptyList(), response))
            val commandResponse = response.receive() as RedisCommandResponse.Error
            commandChannel.close()
            Assert.assertEquals(ioException.message, commandResponse.cause?.message)
        }
    }
}