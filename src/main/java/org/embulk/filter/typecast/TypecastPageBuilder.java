package org.embulk.filter.typecast;

import org.embulk.filter.typecast.cast.*;

import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.*;
import org.msgpack.value.Value;

import org.slf4j.Logger;


class TypecastPageBuilder {
    private static final Logger logger = Exec.getLogger(TypecastFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final JsonVisitor jsonVisitor;

    TypecastPageBuilder(TypecastFilterPlugin.PluginTask task, Schema inputSchema, Schema outputSchema,
            PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        this.jsonVisitor = new JsonVisitor(task, inputSchema, outputSchema);
    }

    public void setFromBoolean(Column outputColumn, boolean value) {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, BooleanCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, BooleanCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, BooleanCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, BooleanCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, BooleanCast.asTimestamp(value));
        } else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, BooleanCast.asJson(value));
        } else {
            assert (false);
        }
    }

    public void setFromLong(Column outputColumn, long value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, LongCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, LongCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, LongCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, LongCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, LongCast.asTimestamp(value));
        } else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, LongCast.asJson(value));
        } else {
            assert(false);
        }
    }

    public void setFromDouble(Column outputColumn, double value)
    {
        try {
            Type outputType = outputColumn.getType();
            if (outputType instanceof BooleanType) {
                pageBuilder.setBoolean(outputColumn, DoubleCast.asBoolean(value));
            } else if (outputType instanceof LongType) {
                pageBuilder.setLong(outputColumn, DoubleCast.asLong(value));
            } else if (outputType instanceof DoubleType) {
                pageBuilder.setDouble(outputColumn, DoubleCast.asDouble(value));
            } else if (outputType instanceof StringType) {
                pageBuilder.setString(outputColumn, DoubleCast.asString(value));
            } else if (outputType instanceof TimestampType) {
                pageBuilder.setTimestamp(outputColumn, DoubleCast.asTimestamp(value));
            } else if (outputType instanceof JsonType) {
                pageBuilder.setJson(outputColumn, DoubleCast.asJson(value));
            } else {
                assert (false);
            }
        }
        catch (DataException ex) {

        }
    }

    public void setFromString(Column outputColumn, String value, TimestampParser timestampParser)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, StringCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, StringCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, StringCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, StringCast.asString(value));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, StringCast.asTimestamp(value, timestampParser));
        } else if (outputType instanceof JsonType) {
            Value jsonValue = StringCast.asJson(value);
            String jsonPath = new StringBuilder("$.").append(outputColumn.getName()).toString();
            Value castedValue = jsonVisitor.visit(jsonPath, jsonValue);
            pageBuilder.setJson(outputColumn, castedValue);
        } else {
            assert(false);
        }
    }

    public void setFromTimestamp(Column outputColumn, Timestamp value, TimestampFormatter timestampFormatter)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, TimestampCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, TimestampCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, TimestampCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, TimestampCast.asString(value, timestampFormatter));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, TimestampCast.asTimestamp(value));
        } else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, TimestampCast.asJson(value));
        } else {
            assert(false);
        }
    }

    public void setFromJson(Column outputColumn, Value value)
    {
        String jsonPath = new StringBuilder("$.").append(outputColumn.getName()).toString();
        Value castedValue = jsonVisitor.visit(jsonPath, value);
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, JsonCast.asBoolean(castedValue));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, JsonCast.asLong(castedValue));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, JsonCast.asDouble(castedValue));
        } else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, JsonCast.asString(castedValue));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, JsonCast.asTimestamp(castedValue));
        } else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, JsonCast.asJson(castedValue));
        } else {
            assert(false);
        }
    }
}
