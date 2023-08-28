package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class LongCast
{
    private LongCast() {}

    private static String buildErrorMessage(String as, long value)
    {
        return String.format("cannot cast long to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(long value)
    {
        if (value == 1) {
            return true;
        }
        else if (value == 0) {
            return false;
        }
        else {
            throw new DataException(buildErrorMessage("boolean", value));
        }
    }

    public static long asLong(long value)
    {
        return value;
    }

    public static double asDouble(long value)
    {
        return (double) value;
    }

    public static String asString(long value)
    {
        return String.valueOf(value);
    }

    public static Value asJson(long value)
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(long value)
    {
        return Timestamp.ofEpochSecond(value);
    }
}
