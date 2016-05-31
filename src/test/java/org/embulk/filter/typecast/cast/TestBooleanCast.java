package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBooleanCast
{
    @Test
    public void asBoolean()
    {
        assertEquals(true, BooleanCast.asBoolean(true));
        assertEquals(false, BooleanCast.asBoolean(false));
    }

    @Test
    public void asLong()
    {
        assertEquals(1, BooleanCast.asLong(true));
        assertEquals(0, BooleanCast.asLong(false));
    }

    @Test(expected = DataException.class)
    public void asDouble()
    {
        BooleanCast.asDouble(true);
    }

    @Test
    public void asString()
    {
        assertEquals("true", BooleanCast.asString(true));
        assertEquals("false", BooleanCast.asString(false));
    }

    @Test(expected = DataException.class)
    public void asJson()
    {
        BooleanCast.asJson(true);
    }

    @Test(expected = DataException.class)
    public void asTimestamp()
    {
        BooleanCast.asTimestamp(true);
    }
}
