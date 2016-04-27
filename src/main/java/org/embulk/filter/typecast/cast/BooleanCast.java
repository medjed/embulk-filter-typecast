package org.embulk.filter.typecast.cast;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.DataException;
import org.msgpack.value.Value;

public class BooleanCast {
    private static String buildErrorMessage(String as, boolean value)
    {
        return String.format("cannot cast double to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(boolean value) throws DataException
    {
        return value;
    }

    public static long asLong(boolean value) throws DataException
    {
        return value ? 1 : 0;
    }

    public static double asDouble(boolean value) throws DataException
    {
        throw new DataException(buildErrorMessage("double", value));
    }

    public static String asString(boolean value) throws DataException
    {
        return value ? "true" : "false";
    }

    public static Value asJson(boolean value) throws DataException
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(boolean value) throws DataException
    {
        throw new DataException(buildErrorMessage("timestamp", value));
    }
}
