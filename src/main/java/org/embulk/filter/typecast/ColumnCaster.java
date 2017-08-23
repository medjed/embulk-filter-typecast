package org.embulk.filter.typecast;

import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.Utils;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PropertyPathToken;
import org.embulk.filter.typecast.TypecastFilterPlugin.ColumnConfig;
import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;

import org.embulk.filter.typecast.cast.BooleanCast;
import org.embulk.filter.typecast.cast.DoubleCast;
import org.embulk.filter.typecast.cast.JsonCast;
import org.embulk.filter.typecast.cast.LongCast;
import org.embulk.filter.typecast.cast.StringCast;
import org.embulk.filter.typecast.cast.TimestampCast;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;

import org.slf4j.Logger;

import java.util.HashMap;

class ColumnCaster
{
    private static final Logger logger = Exec.getLogger(TypecastFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, TimestampParser> timestampParserMap = new HashMap<>();
    private final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<>();
    private final JsonVisitor jsonVisitor;

    ColumnCaster(TypecastFilterPlugin.PluginTask task, Schema inputSchema, Schema outputSchema,
            PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;

        buildTimestampParserMap();
        buildTimestampFormatterMap();
        this.jsonVisitor = new JsonVisitor(task, inputSchema, outputSchema);
    }

    private void buildTimestampParserMap()
    {
        // columnName => TimestampParser
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (PathCompiler.isProbablyJsonPath(columnConfig.getName())) {
                continue; // type: json columns do not support type: timestamp
            }
            Column inputColumn = inputSchema.lookupColumn(columnConfig.getName());
            if (inputColumn.getType() instanceof StringType && columnConfig.getType() instanceof TimestampType) {
                TimestampParser parser = getTimestampParser(columnConfig, task);
                this.timestampParserMap.put(columnConfig.getName(), parser);
            }
        }
    }

    private void buildTimestampFormatterMap()
    {
        // columnName => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (PathCompiler.isProbablyJsonPath(columnConfig.getName())) {
                continue; // type: json columns do not have type: timestamp
            }
            Column inputColumn = inputSchema.lookupColumn(columnConfig.getName());
            if (inputColumn.getType() instanceof TimestampType && columnConfig.getType() instanceof StringType) {
                TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
                this.timestampFormatterMap.put(columnConfig.getName(), parser);
            }
        }
    }

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        return new TimestampParser(format, timezone);
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        return new TimestampFormatter(format, timezone);
    }

    public void setFromBoolean(Column outputColumn, boolean value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, BooleanCast.asBoolean(value));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, BooleanCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, BooleanCast.asDouble(value));
        }
        else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, BooleanCast.asString(value));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, BooleanCast.asTimestamp(value));
        }
        else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, BooleanCast.asJson(value));
        }
        else {
            assert (false);
        }
    }

    public void setFromLong(Column outputColumn, long value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, LongCast.asBoolean(value));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, LongCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, LongCast.asDouble(value));
        }
        else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, LongCast.asString(value));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, LongCast.asTimestamp(value));
        }
        else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, LongCast.asJson(value));
        }
        else {
            assert false;
        }
    }

    public void setFromDouble(Column outputColumn, double value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, DoubleCast.asBoolean(value));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, DoubleCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, DoubleCast.asDouble(value));
        }
        else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, DoubleCast.asString(value));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, DoubleCast.asTimestamp(value));
        }
        else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, DoubleCast.asJson(value));
        }
        else {
            assert false;
        }
    }

    public void setFromString(Column outputColumn, String value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, StringCast.asBoolean(value));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, StringCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, StringCast.asDouble(value));
        }
        else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, StringCast.asString(value));
        }
        else if (outputType instanceof TimestampType) {
            TimestampParser timestampParser = timestampParserMap.get(outputColumn.getName());
            pageBuilder.setTimestamp(outputColumn, StringCast.asTimestamp(value, timestampParser));
        }
        else if (outputType instanceof JsonType) {
            Value jsonValue = StringCast.asJson(value);
            String name = outputColumn.getName();
            String jsonPath = new StringBuilder("$").append(PropertyPathToken.getPathFragment(name)).toString();
            Value castedValue = jsonVisitor.visit(jsonPath, jsonValue);
            pageBuilder.setJson(outputColumn, castedValue);
        }
        else {
            assert false;
        }
    }

    public void setFromTimestamp(Column outputColumn, Timestamp value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, TimestampCast.asBoolean(value));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, TimestampCast.asLong(value));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, TimestampCast.asDouble(value));
        }
        else if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, TimestampCast.asString(value, timestampFormatter));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, TimestampCast.asTimestamp(value));
        }
        else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, TimestampCast.asJson(value));
        }
        else {
            assert false;
        }
    }

    public void setFromJson(Column outputColumn, Value value)
    {
        String name = outputColumn.getName();
        String jsonPath = new StringBuilder("$").append(PropertyPathToken.getPathFragment(name)).toString();
        Value castedValue = jsonVisitor.visit(jsonPath, value);
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, JsonCast.asBoolean(castedValue));
        }
        else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, JsonCast.asLong(castedValue));
        }
        else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, JsonCast.asDouble(castedValue));
        }
        else if (outputType instanceof StringType) {
            pageBuilder.setString(outputColumn, JsonCast.asString(castedValue));
        }
        else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, JsonCast.asTimestamp(castedValue));
        }
        else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, JsonCast.asJson(castedValue));
        }
        else {
            assert false;
        }
    }
}
