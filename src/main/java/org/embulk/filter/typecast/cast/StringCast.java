package org.embulk.filter.typecast.cast;

import org.embulk.spi.DataException;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StringCast {
    private static final JsonParser jsonParser = new JsonParser();

    // copy from csv plugin
    public static final Set<String> TRUE_STRINGS;

    public static final Set<String> FALSE_STRINGS;

    static {
        Set<String> trueStrings = new HashSet<>(Arrays.asList(
                "true", "True", "TRUE",
                "yes", "Yes", "YES",
                "t", "T", "y", "Y",
                "on", "On", "ON",
                "1"));
        TRUE_STRINGS = Collections.unmodifiableSet(trueStrings);

        Set<String> falseStrings = new HashSet<>(Arrays.asList(
                "false", "False", "FALSE",
                "no", "No", "NO",
                "f", "F", "n", "N",
                "off", "Off", "OFF",
                "0"));
        FALSE_STRINGS = Collections.unmodifiableSet(falseStrings);
    }


    private StringCast() {
    }

    private static String buildErrorMessage(String as, String value) {
        return String.format("cannot cast String to %s: \"%s\"", as, value);
    }

    public static boolean asBoolean(String value) {
        if (TRUE_STRINGS.contains(value)) {
            return true;
        } else if (FALSE_STRINGS.contains(value)) {
            return false;
        } else {
            throw new DataException(buildErrorMessage("boolean", value));
        }
    }

    public static long asLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new DataException(buildErrorMessage("long", value), ex);
        }
    }

    public static double asDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ex) {
            throw new DataException(buildErrorMessage("double", value), ex);
        }
    }

    public static String asString(String value) {
        return value;
    }

    public static Value asJson(String value) {
        try {
            return jsonParser.parse(value);
        } catch (JsonParseException ex) {
            throw new DataException(buildErrorMessage("json", value), ex);
        }
    }

    public static Timestamp asTimestamp(String value, TimestampFormatter parser) {
        try {
            return Timestamp.ofInstant(parser.parse(value));
        } catch (DateTimeParseException ex) {
            throw new DataException(buildErrorMessage("timestamp", value), ex);
        }
    }
}
