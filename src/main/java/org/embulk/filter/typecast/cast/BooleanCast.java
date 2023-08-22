package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class BooleanCast
{
    private BooleanCast() {}

    private static String buildErrorMessage(String as, boolean value)
    {
        return String.format("cannot cast boolean to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(boolean value)
    {
        return value;
    }

    public static long asLong(boolean value)
    {
        return value ? 1 : 0;
    }

    public static double asDouble(boolean value)
    {
        throw new DataException(buildErrorMessage("double", value));
    }

    public static String asString(boolean value)
    {
        return value ? "true" : "false";
    }

    public static Value asJson(boolean value)
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(boolean value)
    {
        throw new DataException(buildErrorMessage("timestamp", value));
    }
}
