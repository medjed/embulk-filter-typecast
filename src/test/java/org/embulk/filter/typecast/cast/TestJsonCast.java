package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import static org.junit.Assert.assertEquals;

public class TestJsonCast
{
    public Value value;

    @Before
    public void createResource()
    {
        Value[] kvs = new Value[2];
        kvs[0] = ValueFactory.newString("k");
        kvs[1] = ValueFactory.newString("v");
        value = ValueFactory.newMap(kvs);
    }

    @Test(expected = DataException.class)
    public void asBoolean()
    {
        JsonCast.asBoolean(value);
    }

    @Test(expected = DataException.class)
    public void asLong()
    {
        JsonCast.asLong(value);
    }

    @Test(expected = DataException.class)
    public void asDouble()
    {
        JsonCast.asDouble(value);
    }

    @Test
    public void asString()
    {
        assertEquals("{\"k\":\"v\"}", JsonCast.asString(value));
    }

    @Test
    public void asJson()
    {
        assertEquals(value, JsonCast.asJson(value));
    }

    @Test(expected = DataException.class)
    public void asTimestamp()
    {
        JsonCast.asTimestamp(value);
    }
}
