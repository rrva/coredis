package se.rrva.coredis.protocol

import kotlinx.io.core.BytePacketBuilder

fun BytePacketBuilder.writeRedisCommand(cmd: String, args: List<ByteArray>?) {
    val elements = (args?.size ?: 0) + 1
    append("*")
    append("$elements")
    append("\r\n")
    append("$")
    append("${cmd.length}")
    append("\r\n")
    append(cmd)
    append("\r\n")
    args?.forEach {
        append("$")
        append("${it.size}")
        append("\r\n")
        writeFully(it, 0, it.size)
        append("\r\n")
    }
}
