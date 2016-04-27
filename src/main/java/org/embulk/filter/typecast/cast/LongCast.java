package org.embulk.filter.typecast.cast;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.DataException;
import org.msgpack.value.Value;

public class LongCast {
    private static String buildErrorMessage(String as, long value)
    {
        return String.format("cannot cast long to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(long value) throws DataException
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

    public static long asLong(long value) throws DataException
    {
        return value;
    }

    public static double asDouble(long value) throws DataException
    {
        return (double)value;
    }

    public static String asString(long value) throws DataException
    {
        return String.valueOf(value);
    }

    public static Value asJson(long value) throws DataException
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(long value) throws DataException
    {
        return Timestamp.ofEpochSecond(value);
    }
}
