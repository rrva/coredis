import org.junit.Assert
import org.junit.Test
import se.rrva.coredis.protocol.RedisParser

class RedisParserTest {

    @Test
    fun `simple string`() {
        val p = RedisParser()
        p.feed("+OK", 3)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("OK", p.message())
    }

    @Test
    fun `error string`() {
        val p = RedisParser()
        p.feed("-Error message", 14)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("Error message", p.message())
    }

    @Test
    fun `integer`() {
        val p = RedisParser()
        p.feed(":1000", 5)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("1000", p.message())
    }

    @Test
    fun `bulk string`() {
        val p = RedisParser()
        p.feed("\$6", 2)
        Assert.assertEquals(true, p.needsMore())
        p.feed("foobar", 6)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("foobar", p.message())
    }

    @Test
    fun `empty bulk string`() {
        val p = RedisParser()
        p.feed("\$0", 2)
        p.feed("", 0)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("", p.message())
    }

    @Test
    fun `null bulk string`() {
        val p = RedisParser()
        p.feed("\$-1", 3)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals(null, p.message())
    }

    @Test
    fun `bulk string containing multiple lines`() {
        val p = RedisParser()
        p.feed("\$4", 2)
        p.feed("a", 1)
        p.feed("b", 1)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("a\r\nb", p.message())
    }

    @Test
    fun `bulk string containing utf-8 which spans more bytes than characters`() {
        val p = RedisParser()
        p.feed("\$11", 3)
        p.feed("κόσμε", 11)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("κόσμε", p.message())
    }

    @Test
    fun `simple string containing utf-8 which spans more bytes than characters`() {
        val p = RedisParser()
        p.feed("+κόσμε", 12)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals("κόσμε", p.message())
    }

    @Test
    fun `malformed input`() {
        val p = RedisParser()
        p.feed("foo", 3)
        Assert.assertEquals(false, p.needsMore())
        Assert.assertEquals(true, p.hasError())
    }
}
