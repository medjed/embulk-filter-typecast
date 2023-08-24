package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class DoubleCast
{
    private DoubleCast() {}

    private static String buildErrorMessage(String as, double value)
    {
        return String.format("cannot cast double to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(double value)
    {
        throw new DataException(buildErrorMessage("boolean", value));
    }

    public static long asLong(double value)
    {
        return (long) value;
    }

    public static double asDouble(double value)
    {
        return value;
    }

    public static String asString(double value)
    {
        return String.valueOf(value);
    }

    public static Value asJson(double value)
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(double value)
    {
        long epochSecond = (long) value;
        long nanoAdjustMent = (long) ((value - epochSecond) * 1000000000);
        return Timestamp.ofEpochSecond(epochSecond, nanoAdjustMent);
    }
}
