package org.embulk.filter.typecast.cast;

import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStringCast
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResource()
    {
    }

    @Test
    public void asBoolean()
    {
        for (String str : StringCast.TRUE_STRINGS) {
            assertEquals(true, StringCast.asBoolean(str));
        }
        for (String str : StringCast.FALSE_STRINGS) {
            assertEquals(false, StringCast.asBoolean(str));
        }
        try {
            StringCast.asBoolean("foo");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asLong()
    {
        assertEquals(1, StringCast.asLong("1"));
        try {
            StringCast.asLong("1.5");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
        try {
            StringCast.asLong("foo");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asDouble()
    {
        assertEquals(1.0, StringCast.asDouble("1"), 0.0);
        assertEquals(1.5, StringCast.asDouble("1.5"), 0.0);
        try {
            StringCast.asDouble("foo");
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asString()
    {
        assertEquals("1", StringCast.asString("1"));
        assertEquals("1.5", StringCast.asString("1.5"));
        assertEquals("foo", StringCast.asString("foo"));
    }

    @Test
    public void asJson()
    {
        Value[] kvs = new Value[2];
        kvs[0] = ValueFactory.newString("k");
        kvs[1] = ValueFactory.newString("v");
        Value value = ValueFactory.newMap(kvs);
        assertEquals(value, StringCast.asJson("{\"k\":\"v\"}"));
    }

    @Test
    public void asTimestamp()
    {
        Timestamp expected = Timestamp.ofEpochSecond(1463084053, 123456000);
        TimestampParser parser = new TimestampParser("%Y-%m-%d %H:%M:%S.%N", DateTimeZone.UTC);
        assertEquals(expected, StringCast.asTimestamp("2016-05-12 20:14:13.123456", parser));

        try {
            StringCast.asTimestamp("foo", parser);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }
}
