package se.rrva.coredis

import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.ExperimentalCoroutinesApi

interface Redis {
    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun send(cmd: String, vararg args: String): String

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun sendNullable(cmd: String, vararg args: String): String?

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun sendNullable(cmd: String, vararg args: ByteArray): String?

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun ping(vararg echoText: String): String

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun set(key: String, value: String): String

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun setex(key: String, value: String, expireSeconds: Long): String

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun get(key: String): String?

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun del(key: String): String

    @ExperimentalCoroutinesApi
    @KtorExperimentalAPI
    suspend fun ttl(key: String): String
}

class RedisException(msg: String, cause: Throwable?) : Exception(msg, cause)
