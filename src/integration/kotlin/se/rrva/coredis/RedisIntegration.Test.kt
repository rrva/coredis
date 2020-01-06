package se.rrva.coredis

import eu.rekawek.toxiproxy.model.ToxicDirection
import io.ktor.util.KtorExperimentalAPI
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.debug.junit4.CoroutinesTimeout
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.*
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.ToxiproxyContainer

/**
 * Integration tests using a real redis instance, running with Docker
 */
class RedisIntegrationTest {

    private lateinit var redis: Redis

    @Rule
    @JvmField
    val timeout = CoroutinesTimeout.seconds(10)

    companion object {

        private val network = Network.newNetwork()
        private val redisServer: GenericContainer<*> =
            KGenericContainer("redis:5.0.7-alpine").withExposedPorts(6379).withNetwork(
                network
            )
        private val toxiProxy: ToxiproxyContainer = ToxiproxyContainer().withNetwork(network)
        lateinit var proxy: ToxiproxyContainer.ContainerProxy

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            redisServer.start()
            toxiProxy.start()
            proxy = toxiProxy.getProxy(
                redisServer, 6379)
        }
    }

    @Before
    fun before() {
        redis = RedisClient(InetSocketAddress(proxy.containerIpAddress, proxy.proxyPort), 1000, 1000)
    }

    @After
    fun after() {
        proxy.toxics().all.forEach {
            it.remove()
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `can connect to a redis server, send ping and get a response`() {
        runBlocking {
            val reply = redis.ping()
            Assert.assertEquals("PONG", reply)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `unique ping message is sent and received`() {
        runBlocking {
            val uniquePingMessage = UUID.randomUUID().toString()
            val reply = redis.ping(uniquePingMessage)
            Assert.assertEquals(uniquePingMessage, reply)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `set a key and get a key`() {
        runBlocking {
            val expected = UUID.randomUUID().toString()
            val key = "test-set"
            redis.set(key, expected)
            val actual = redis.get(key)
            Assert.assertEquals(expected, actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `set a utf8 value and get it uncorrupted back`() {
        runBlocking {
            val expected = "κόσμε"
            val key = "test-set"
            val setResult = redis.set(key, expected)
            Assert.assertEquals("OK", setResult)
            val actual = redis.get(key)
            Assert.assertEquals(expected, actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `setting a key with crlf in the value works to read back unbroken`() {
        runBlocking {
            val expected = "hello\r\nworld\r\nfoo"
            val key = "test-crlf"
            redis.set(key, expected)
            val actual = redis.get(key)
            Assert.assertEquals(expected, actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `setting a key, then deleting it and getting it gives null`() {
        runBlocking {
            val key = "test-todelete"
            redis.set(key, "somevalue")
            val actual = redis.get(key)
            Assert.assertEquals("somevalue", actual)
            redis.del(key)
            val newValue = redis.get(key)
            Assert.assertEquals(null, newValue)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `setting a key with lf in the value works to read back unbroken`() {
        runBlocking {
            val expected = "hello\nworld\nfoo"
            val key = "test-crlf"
            redis.set(key, expected)
            val actual = redis.get(key)
            Assert.assertEquals(expected, actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `setting a 1mb key works to read back unbroken`() {
        runBlocking {
            val sb = StringBuilder()
            repeat(1024 * 1024) {
                sb.append((it % 255).toChar())
            }
            val key = "test-large"
            val large1MbValue = sb.toString()
            redis.set(key, large1MbValue)
            val actual = redis.get(key)
            Assert.assertEquals(large1MbValue, actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `getting a a non existing key gives null`() {
        runBlocking {
            val actual = redis.get("nonexisting")
            Assert.assertEquals(null, actual)
        }
    }

    @Test(expected = RedisException::class)
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `propagates exception due to timeout back to caller`() {
        proxy.toxics().timeout("timeout", ToxicDirection.DOWNSTREAM, 100)
        runBlocking {
            redis.get("test-timeout")
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `recovers after connection closed`() {
        runBlocking {
            val key = "test-timeout"
            redis.set(key, "foo")
            proxy.toxics().timeout("timeout", ToxicDirection.DOWNSTREAM, 0)
            try {
                redis.get(key)
            } catch (ignore: RedisException) {
                println("Got exception $ignore")
            }
            proxy.toxics().get("timeout").remove()
            val actual = redis.get(key)
            Assert.assertEquals("foo", actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `recovers after downstream timeout`() {
        runBlocking {
            val key = "test-timeout-2"
            redis.set(key, "foo")
            proxy.toxics().timeout("timeout", ToxicDirection.DOWNSTREAM, 100)
            try {
                redis.get(key)
            } catch (ignore: RedisException) {
                println("Got exception $ignore")
            }
            proxy.toxics().get("timeout").remove()
            delay(200)
            val actual = redis.get(key)
            Assert.assertEquals("foo", actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `recovers after upstream timeout`() {
        runBlocking {
            val key = "test-timeout-3"
            redis.set(key, "foo")
            proxy.toxics().timeout("timeout", ToxicDirection.UPSTREAM, 100)
            try {
                redis.get(key)
            } catch (ignore: RedisException) {
                println("Got exception $ignore")
            }
            proxy.toxics().get("timeout").remove()
            delay(200)
            val actual = redis.get(key)
            Assert.assertEquals("foo", actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `setex sets a ttl`() {
        runBlocking {
            val key = "test-setex"
            redis.setex(key, "test", 17)
            val actual = redis.ttl(key)
            Assert.assertEquals("17", actual)
        }
    }

    @Test
    @KtorExperimentalAPI
    @ExperimentalCoroutinesApi
    fun `concurrent set and get do not mix up values, pipelining and channels work`() {
        val coroutineId = AtomicLong()
        runBlocking {
            repeat(100) {
                async {
                    val myId = coroutineId.incrementAndGet()
                    val expectedValue = "bar-$myId"
                    val myKey = "foo-$myId"
                    redis.setex(myKey, expectedValue, 10)
                    val actualValue = redis.get(myKey)
                    Assert.assertEquals(expectedValue, actualValue)
                }
            }
        }
    }
}

class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
