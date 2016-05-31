package org.embulk.filter.typecast.cast;

import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTimestampCast
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    public ScriptingContainer jruby;
    public Timestamp timestamp;

    @Before
    public void createResource()
    {
        timestamp = Timestamp.ofEpochSecond(1463084053, 500000000);
        jruby = new ScriptingContainer();
    }

    @Test(expected = DataException.class)
    public void asBoolean()
    {
        TimestampCast.asBoolean(timestamp);
    }

    @Test
    public void asLong()
    {
        assertEquals(timestamp.getEpochSecond(), TimestampCast.asLong(timestamp));
    }

    @Test
    public void asDouble()
    {
        double unixtimestamp = timestamp.getEpochSecond() + timestamp.getNano() / 1000000000.0;
        assertEquals(unixtimestamp, TimestampCast.asDouble(timestamp), 0.0);
    }

    @Test
    public void asString()
    {
        TimestampFormatter formatter = new TimestampFormatter(jruby, "%Y-%m-%d %H:%M:%S.%6N", DateTimeZone.UTC);
        assertEquals("2016-05-12 20:14:13.500000", TimestampCast.asString(timestamp, formatter));
    }

    @Test(expected = DataException.class)
    public void asJson()
    {
        TimestampCast.asJson(timestamp);
    }

    @Test
    public void asTimestamp()
    {
        assertEquals(timestamp, TimestampCast.asTimestamp(timestamp));
    }
}
