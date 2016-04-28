package org.embulk.filter.typecast;

import org.embulk.filter.typecast.cast.*;

import org.embulk.spi.DataException;
import org.embulk.spi.type.*;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

class TypecastJsonBuilder {
    static Value getFromBoolean(Type outputType, BooleanValue value) {
        if (outputType instanceof BooleanType) {
            return value;
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(BooleanCast.asLong(value.getBoolean()));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(BooleanCast.asDouble(value.getBoolean()));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(BooleanCast.asString(value.getBoolean()));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast boolean to json: \"%s\"", value));
        } else {
            assert (false);
            return null;
        }
    }

    static Value getFromLong(Type outputType, IntegerValue value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(LongCast.asBoolean(value.asLong()));
        } else if (outputType instanceof LongType) {
            return value;
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(LongCast.asDouble(value.asLong()));
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(LongCast.asString(value.asLong()));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast long to json:: \"%s\"", value));
        } else {
            assert(false);
            return null;
        }
    }

    static Value getFromDouble(Type outputType, FloatValue value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(DoubleCast.asBoolean(value.toDouble()));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(DoubleCast.asLong(value.toDouble()));
        } else if (outputType instanceof DoubleType) {
            return value;
        } else if (outputType instanceof StringType) {
            return ValueFactory.newString(DoubleCast.asString(value.toDouble()));
        } else if (outputType instanceof JsonType) {
            throw new DataException(String.format("cannot cast double to json:: \"%s\"", value));
        } else {
            assert (false);
            return null;
        }
    }

    static Value getFromString(Type outputType, StringValue value)
    {
        if (outputType instanceof BooleanType) {
            return ValueFactory.newBoolean(StringCast.asBoolean(value.asString()));
        } else if (outputType instanceof LongType) {
            return ValueFactory.newInteger(StringCast.asLong(value.asString()));
        } else if (outputType instanceof DoubleType) {
            return ValueFactory.newFloat(StringCast.asDouble(value.asString()));
        } else if (outputType instanceof StringType) {
            return value;
        } else if (outputType instanceof JsonType) {
            return StringCast.asJson(value.asString());
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
        } else if (outputType instanceof JsonType) {
            return value;
        } else {
            assert(false);
            return null;
        }
    }
}
