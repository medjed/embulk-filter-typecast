package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.value.Value;

public class TimestampCast {
    private static String buildErrorMessage(String as, Timestamp value)
    {
        return String.format("cannot cast Timestamp to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(Timestamp value) throws DataException
    {
        throw new DataException(buildErrorMessage("boolean", value));
    }

    public static long asLong(Timestamp value) throws DataException
    {
        return value.getEpochSecond();
    }

    public static double asDouble(Timestamp value) throws DataException
    {
        long epochSecond = value.getEpochSecond();
        long nano = value.getNano();
        return epochSecond + ((double)nano / 1000000000.0);
    }

    public static String asString(Timestamp value, TimestampFormatter formatter) throws DataException
    {
        return formatter.format(value);
    }

    public static Value asJson(Timestamp value) throws DataException
    {
        throw new DataException(buildErrorMessage("json", value));
    }

    public static Timestamp asTimestamp(Timestamp value) throws DataException
    {
        return value;
    }
}
