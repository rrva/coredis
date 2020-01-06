package se.rrva.coredis.protocol

import java.nio.ByteBuffer
import java.nio.CharBuffer
import kotlinx.coroutines.io.ByteReadChannel
import kotlinx.coroutines.io.readUntilDelimiter
import kotlinx.coroutines.io.skipDelimiter
import kotlinx.io.charsets.CharsetDecoder

private val crlf: ByteBuffer = ByteBuffer.wrap("\r\n".toByteArray())

suspend fun ByteReadChannel.readCRLFLine(
    buffer: ByteBuffer,
    charBuffer: CharBuffer,
    charsetDecoder: CharsetDecoder,
    stringBuilder: StringBuilder = StringBuilder(),
    bytesPreviouslyRead: Int = 0
): Pair<String, Int> {
    buffer.clear()
    val bytesRead = this.readUntilDelimiter(crlf, buffer)
    buffer.flip()
    return when {
        bytesRead == 0 -> {
            this.skipDelimiter(crlf)
            Pair(stringBuilder.toString(), bytesPreviouslyRead)
        }
        bytesRead < buffer.limit() -> {
            charBuffer.clear()
            charsetDecoder.decode(buffer, charBuffer, true)
            charBuffer.flip()
            stringBuilder.append(charBuffer)
            this.skipDelimiter(crlf)
            Pair(stringBuilder.toString(), bytesPreviouslyRead + bytesRead)
        }
        else -> {
            charBuffer.clear()
            charsetDecoder.decode(buffer, charBuffer, false)
            charBuffer.flip()
            stringBuilder.append(charBuffer)
            readCRLFLine(buffer, charBuffer, charsetDecoder, stringBuilder, bytesPreviouslyRead + bytesRead)
        }
    }
}
