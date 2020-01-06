package se.rrva.coredis.protocol

/**
 * Parse the redis RESP protocol. See https://redis.io/topics/protocol
 *
 * Does not handle all data types, only simple strings, bulk strings, error messages and integers.
 *
 * Should use byte arrays instead of strings to avoid implying an encoding
 */
class RedisParser {
    enum class State {
        FirstLine,
        BulkString,
        Done,
        Error
    }

    private var state: State = State.FirstLine
    private var message: String? = null
    private var bulkStringLen: Int = -1
    private var bulkStringBuilder: StringBuilder? = null

    fun feed(line: String, lengthInBytes: Int) {
        when (state) {
            State.FirstLine -> {
                if (lengthInBytes == 0) {
                    throw IllegalStateException("Expected at least 1 bytes of data, got 0")
                }
                val value = line.substring(1)
                when {
                    line.startsWith("$") -> {
                        state = State.BulkString
                        bulkStringLen = value.toInt()
                        if (bulkStringLen == -1) {
                            state = State.Done
                            message = null
                        } else {
                            bulkStringBuilder = StringBuilder(bulkStringLen)
                        }
                    }
                    line.startsWith("+") -> {
                        state = State.Done
                        message = value
                    }
                    line.startsWith("-") -> {
                        state = State.Error
                        message = value
                    }
                    line.startsWith(":") -> {
                        state = State.Done
                        message = value
                    }
                    else -> {
                        state = State.Error
                        message = "Got malformed input '$line'"
                    }
                }
            }
            State.BulkString -> {
                when {
                    bulkStringLen - lengthInBytes > 0 -> {
                        bulkStringBuilder?.append(line)
                        bulkStringBuilder?.append("\r\n")
                        bulkStringLen -= (lengthInBytes + 2)
                    }
                    bulkStringLen - lengthInBytes == 0 -> {
                        bulkStringBuilder?.append(line)
                        message = bulkStringBuilder.toString()
                        state = State.Done
                    }
                    else -> {
                        message =
                            "Invalid bulk string data received, " +
                                    "expected $bulkStringLen bytes but got $lengthInBytes bytes"
                        state = State.Error
                    }
                }
            }
            else -> throw IllegalStateException("Received more data: $line when state was $state")
        }
    }

    fun hasError(): Boolean {
        return state == State.Error
    }

    fun needsMore(): Boolean {
        return state != State.Done && state != State.Error
    }

    fun message(): String? {
        return message
    }
}
