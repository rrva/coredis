import java.nio.charset.Charset
import kotlinx.io.core.BytePacketBuilder
import org.junit.Assert
import org.junit.Test
import se.rrva.coredis.protocol.writeRedisCommand

class RedisPacketTest {

    private val utf8: Charset = Charset.forName("utf8")

    @Test
    fun `writes LLEN mylist packet example from RESP specification correctly`() {
        val bytePacketBuilder = BytePacketBuilder()
        bytePacketBuilder.writeRedisCommand("LLEN", listOf("mylist".toByteArray()))
        val packet = bytePacketBuilder.build()
        val expected = "*2\r\n\$4\r\nLLEN\r\n\$6\r\nmylist\r\n"
        val dst = ByteArray(expected.length)
        packet.readAvailable(dst)
        Assert.assertEquals(expected, dst.toString(utf8))
    }

    @Test
    fun `writes set key value packet correctly`() {
        val bytePacketBuilder = BytePacketBuilder()
        bytePacketBuilder.writeRedisCommand("set", listOf("mykey".toByteArray(), "value".toByteArray()))
        val packet = bytePacketBuilder.build()
        val expected = "*3\r\n\$3\r\nset\r\n\$5\r\nmykey\r\n$5\r\nvalue\r\n"
        val dst = ByteArray(expected.length)
        packet.readAvailable(dst)
        Assert.assertEquals(expected, dst.toString(utf8))
    }

    @Test
    fun `writes get key packet correctly`() {
        val bytePacketBuilder = BytePacketBuilder()
        bytePacketBuilder.writeRedisCommand("get", listOf("mykey".toByteArray()))
        val packet = bytePacketBuilder.build()
        val expected = "*2\r\n\$3\r\nget\r\n\$5\r\nmykey\r\n"
        val dst = ByteArray(expected.length)
        packet.readAvailable(dst)
        Assert.assertEquals(expected, dst.toString(utf8))
    }

    @Test
    fun `writes set key utf8 value packet correctly`() {
        val bytePacketBuilder = BytePacketBuilder()
        bytePacketBuilder.writeRedisCommand("set", listOf("key".toByteArray(), "κόσμε".toByteArray()))
        val packet = bytePacketBuilder.build()
        val expected = "*3\r\n\$3\r\nset\r\n\$3\r\nkey\r\n$11\r\nκόσμε\r\n"
        val dst = ByteArray(expected.toByteArray().size)
        packet.readAvailable(dst)
        Assert.assertEquals(expected, dst.toString(utf8))
    }
}
