package org.embulk.filter.typecast;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.filter.typecast.cast.BooleanCast;
import org.embulk.filter.typecast.cast.DoubleCast;
import org.embulk.filter.typecast.cast.JsonCast;
import org.embulk.filter.typecast.cast.LongCast;
import org.embulk.filter.typecast.cast.StringCast;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class JsonNodeCaster
{
    public static final JsonParser JSON_PARSER = new JsonParser();

    public Object castTo(JsonNode jsonNode, Type outputType, TimestampFormatter formatter)
    {
        switch (jsonNode.getNodeType()) {
            case STRING:
                return stringTo(jsonNode, outputType, formatter);
            case NUMBER:
                if (jsonNode.isIntegralNumber()) {
                    return longTo(jsonNode, outputType, formatter);
                } else {
                    return doubleTo(jsonNode, outputType, formatter);
                }
            case BOOLEAN:
                return booleanTo(jsonNode, outputType, formatter);
            case NULL:
                return null;
            case ARRAY:
            case OBJECT:
                jsonTo(jsonNode, outputType, formatter);
        }
        return null;
    }

    public Object stringTo(JsonNode value, Type outputType, TimestampFormatter formatter)
    {
        String jsonNodeValue = value.asText();
        if (outputType instanceof BooleanType) {
            return StringCast.asBoolean(jsonNodeValue);
        } else if (outputType instanceof DoubleType) {
            return StringCast.asDouble(jsonNodeValue);
        } else if (outputType instanceof LongType) {
            return StringCast.asLong(jsonNodeValue);
        } else if (outputType instanceof StringType) {
            return StringCast.asString(jsonNodeValue);
        } else if (outputType instanceof TimestampType) {
            return StringCast.asTimestamp(jsonNodeValue, formatter);
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            // never come
            return null;
        }
    }

    public Object longTo(JsonNode value, Type outputType, TimestampFormatter formatter)
    {
        long jsonNodeValue = value.asLong();
        if (outputType instanceof BooleanType) {
            return LongCast.asBoolean(jsonNodeValue);
        } else if (outputType instanceof DoubleType) {
            return LongCast.asDouble(jsonNodeValue);
        } else if (outputType instanceof LongType) {
            return LongCast.asLong(jsonNodeValue);
        } else if (outputType instanceof StringType) {
            return LongCast.asString(jsonNodeValue);
        } else if (outputType instanceof TimestampType) {
            return LongCast.asTimestamp(jsonNodeValue);
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            // never come
            return null;
        }
    }

    public Object doubleTo(JsonNode value, Type outputType, TimestampFormatter formatter)
    {
        double jsonNodeValue = value.asDouble();
        if (outputType instanceof BooleanType) {
            return DoubleCast.asBoolean(jsonNodeValue);
        } else if (outputType instanceof DoubleType) {
            return DoubleCast.asDouble(jsonNodeValue);
        } else if (outputType instanceof LongType) {
            return DoubleCast.asLong(jsonNodeValue);
        } else if (outputType instanceof StringType) {
            return DoubleCast.asString(jsonNodeValue);
        } else if (outputType instanceof TimestampType) {
            return DoubleCast.asTimestamp(jsonNodeValue);
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            // never come
            return null;
        }
    }

    public Object booleanTo(JsonNode value, Type outputType, TimestampFormatter formatter)
    {
        boolean jsonNodeValue = value.asBoolean();
        if (outputType instanceof BooleanType) {
            return BooleanCast.asBoolean(jsonNodeValue);
        } else if (outputType instanceof DoubleType) {
            return BooleanCast.asDouble(jsonNodeValue);
        } else if (outputType instanceof LongType) {
            return BooleanCast.asLong(jsonNodeValue);
        } else if (outputType instanceof StringType) {
            return BooleanCast.asString(jsonNodeValue);
        } else if (outputType instanceof TimestampType) {
            return BooleanCast.asTimestamp(jsonNodeValue);
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            // never come
            return null;
        }
    }


    public Object jsonTo(JsonNode value, Type outputType, TimestampFormatter formatter)
    {
        Value jsonNodeValue = JSON_PARSER.parse(value.toString());

        if (outputType instanceof BooleanType) {
            return JsonCast.asBoolean(jsonNodeValue);
        } else if (outputType instanceof DoubleType) {
            return JsonCast.asDouble(jsonNodeValue);
        } else if (outputType instanceof LongType) {
            return JsonCast.asLong(jsonNodeValue);
        } else if (outputType instanceof StringType) {
            return JsonCast.asString(jsonNodeValue);
        } else if (outputType instanceof TimestampType) {
            return JsonCast.asTimestamp(jsonNodeValue);
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            // never come
            return null;
        }
    }
}
