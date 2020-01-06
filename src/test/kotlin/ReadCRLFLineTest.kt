import io.ktor.util.cio.write
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.CharBuffer
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.io.ByteChannel
import kotlinx.coroutines.io.ByteReadChannel
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import se.rrva.coredis.protocol.readCRLFLine

class ReadCRLFLineTest {

    private val utf8 = Charsets.UTF_8

    @Test
    fun `reads line by line`() {
        val bc = ByteReadChannel("hello\r\nworld\r\n")
        val buf = ByteBuffer.allocate(16)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        val (line1, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        val (line2, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        Assert.assertEquals("hello", line1)
        Assert.assertEquals("world", line2)
    }

    @Test(expected = IOException::class)
    fun `reading past input gives exception`() {
        val bc = ByteReadChannel("hello\r\nworld\r\n")
        val buf = ByteBuffer.allocate(16)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
    }

    @Test
    fun `lines are only terminated by crlf`() {
        val bc = ByteReadChannel("hello\nworld\r\n")
        val buf = ByteBuffer.allocate(16)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        val (line1, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        Assert.assertEquals("hello\nworld", line1)
    }

    @Test
    fun `crlf split across destination buffers`() {
        val bc = ByteReadChannel("hello\r\nworld\r\n")
        val buf = ByteBuffer.allocate(6)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        val (line1, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        Assert.assertEquals("hello", line1)
    }

    @Test
    fun `crlf begins in next destination buffer`() {
        val bc = ByteReadChannel("hello\r\nworld\r\n")
        val buf = ByteBuffer.allocate(5)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        val (line1, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        Assert.assertEquals("hello", line1)
    }

    @Test
    fun `handles buffer overflow`() {
        val bc = ByteReadChannel("hello\r\nworld\r\n")
        val buf = ByteBuffer.allocate(4)
        val cb = CharBuffer.allocate(4)
        val charsetDecoder = utf8.newDecoder()
        val (line1, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        val (line2, _) = runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
        Assert.assertEquals("hello", line1)
        Assert.assertEquals("world", line2)
    }

    @Test(expected = IOException::class)
    fun `eof occurs before end of line`() {
        val bc = ByteReadChannel("hello")
        val buf = ByteBuffer.allocate(16)
        val cb = CharBuffer.allocate(16)
        val charsetDecoder = utf8.newDecoder()
        runBlocking { bc.readCRLFLine(buf, cb, charsetDecoder) }
    }

    @Test
    fun `waits until delimiter encountered`() {
        runBlocking {
            val bc = ByteChannel()
            val buf = ByteBuffer.allocate(16)
            val cb = CharBuffer.allocate(16)
            val charsetDecoder = utf8.newDecoder()
            async {
                bc.write("hello\r")
                bc.flush()
                delay(10)
                bc.write("\ntrailing")
                bc.flush()
                delay(10)
                bc.write("\r\n")
                bc.flush()
            }
            val (line1, _) = bc.readCRLFLine(buf, cb, charsetDecoder)

            Assert.assertEquals("hello", line1)
        }
    }
}
