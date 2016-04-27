package org.embulk.filter.typecast;

import org.embulk.filter.typecast.cast.*;

import org.embulk.spi.DataException;
import org.embulk.spi.type.*;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

class TypecastJsonBuilder {
    static Value getFromBoolean(Type outputType, boolean value) {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(value);
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(BooleanCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(BooleanCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(BooleanCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            throw new DataException(String.format("no timestamp type in json: \"%s\"", value));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast boolean to json: \"%s\"", value));
        } else {
            assert (false);
            return null;
        }
    }

    static Value getFromLong(Type outputType, long value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(LongCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(value);
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(LongCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(LongCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            throw new DataException(String.format("no timestamp type in json: \"%s\"", value));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast long to json:: \"%s\"", value));
        } else {
            assert(false);
            return null;
        }
    }

    static Value getFromDouble(Type outputType, double value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(DoubleCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(DoubleCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(DoubleCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(DoubleCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            throw new DataException(String.format("no timestamp type in json: \"%s\"", value));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast double to json:: \"%s\"", value));
        } else {
            assert (false);
            return null;
        }
    }

    static Value getFromString(Type outputType, String value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(StringCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(StringCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(StringCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(StringCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            throw new DataException(String.format("no timestamp type in json: \"%s\"", value));
        } else if (outputType instanceof JsonType) {
            return StringCast.asJson(value);
        } else {
            assert(false);
            return null;
        }
    }

    static Value getFromJson(Type outputType, Value value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(JsonCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(JsonCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(JsonCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(JsonCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            throw new DataException(String.format("no timestamp type in json: \"%s\"", value));
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            assert(false);
            return null;
        }
    }
}
