package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class TestTimestampCast
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    public Timestamp timestamp;

    @Before
    public void createResource()
    {
        timestamp = Timestamp.ofEpochSecond(1463084053, 500000000);
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
        TimestampFormatter formatter = org.embulk.util.timestamp.TimestampFormatter
                .builder("%Y-%m-%d %H:%M:%S.%N", true)
                .setDefaultZoneId(ZoneId.of("UTC"))
                .build();
        assertEquals("2016-05-12 20:14:13.500000000", TimestampCast.asString(timestamp, formatter));
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
