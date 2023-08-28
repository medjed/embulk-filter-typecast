package org.embulk.filter.typecast;

import org.embulk.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JsonPathUtilTest
{

    @Test
    public void testIsProbablyJsonPath()
    {
        assertTrue(JsonPathUtil.isProbablyJsonPath("$.foo"));
        assertFalse(JsonPathUtil.isProbablyJsonPath("bar"));
    }

    @Test
    public void testGetColumnName()
    {
        assertEquals("foo", JsonPathUtil.getColumnName("$.foo"));
        assertEquals("foo", JsonPathUtil.getColumnName("$.foo.bar"));
        assertEquals("foo", JsonPathUtil.getColumnName("$.foo.bar[0].name"));
    }

    @Test
    public void testAssertJsonPathFormat()
    {
        JsonPathUtil.assertJsonPathFormat("$.foo");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar[0]");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar[-1]");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar[0:2]");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar[*]");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar[*].name");
        JsonPathUtil.assertJsonPathFormat("$.foo.bar.*");
        assertThrows(ConfigException.class, () -> JsonPathUtil.assertJsonPathFormat("length($.foo.bar.*)"));
    }
}