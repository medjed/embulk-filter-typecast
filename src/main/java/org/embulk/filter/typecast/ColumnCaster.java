package org.embulk.filter.typecast;

import com.google.common.base.Optional;

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
import org.jruby.embed.ScriptingContainer;
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
                TimestampParser parser = createTimestampParser(task, columnConfig);
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
                TimestampFormatter parser = createTimestampFormatter(task, columnConfig);
                this.timestampFormatterMap.put(columnConfig.getName(), parser);
            }
        }
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

    private TimestampFormatter createTimestampFormatter(PluginTask task, ColumnConfig columnConfig)
    {
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        return createTimestampFormatter(format, timezone);
    }

    private TimestampParser createTimestampParser(PluginTask task, ColumnConfig columnConfig)
    {
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        String date = columnConfig.getDate().or(task.getDefaultDate());
        return createTimestampParser(format, timezone, date);
    }

    private class TimestampFormatterTaskImpl implements TimestampFormatter.Task
    {
        private final DateTimeZone defaultTimeZone;
        private final String defaultTimestampFormat;
        public TimestampFormatterTaskImpl(
                DateTimeZone defaultTimeZone,
                String defaultTimestampFormat)
        {
            this.defaultTimeZone = defaultTimeZone;
            this.defaultTimestampFormat = defaultTimestampFormat;
        }
        @Override
        public DateTimeZone getDefaultTimeZone()
        {
            return this.defaultTimeZone;
        }
        @Override
        public String getDefaultTimestampFormat()
        {
            return this.defaultTimestampFormat;
        }
        @Override
        public ScriptingContainer getJRuby()
        {
            return null;
        }
    }

    private class TimestampFormatterColumnOptionImpl implements TimestampFormatter.TimestampColumnOption
    {
        private final Optional<DateTimeZone> timeZone;
        private final Optional<String> format;
        public TimestampFormatterColumnOptionImpl(
                Optional<DateTimeZone> timeZone,
                Optional<String> format)
        {
            this.timeZone = timeZone;
            this.format = format;
        }
        @Override
        public Optional<DateTimeZone> getTimeZone()
        {
            return this.timeZone;
        }
        @Override
        public Optional<String> getFormat()
        {
            return this.format;
        }
    }

    // ToDo: Replace with `new TimestampFormatter(format, timezone)`
    // after deciding to drop supporting embulk < 0.8.29.
    private TimestampFormatter createTimestampFormatter(String format, DateTimeZone timezone)
    {
        TimestampFormatterTaskImpl task = new TimestampFormatterTaskImpl(timezone, format);
        TimestampFormatterColumnOptionImpl columnOption = new TimestampFormatterColumnOptionImpl(
                Optional.of(timezone), Optional.of(format));
        return new TimestampFormatter(task, Optional.of(columnOption));
    }

    private class TimestampParserTaskImpl implements TimestampParser.Task
    {
        private final DateTimeZone defaultTimeZone;
        private final String defaultTimestampFormat;
        private final String defaultDate;
        public TimestampParserTaskImpl(
                DateTimeZone defaultTimeZone,
                String defaultTimestampFormat,
                String defaultDate)
        {
            this.defaultTimeZone = defaultTimeZone;
            this.defaultTimestampFormat = defaultTimestampFormat;
            this.defaultDate = defaultDate;
        }
        @Override
        public DateTimeZone getDefaultTimeZone()
        {
            return this.defaultTimeZone;
        }
        @Override
        public String getDefaultTimestampFormat()
        {
            return this.defaultTimestampFormat;
        }
        @Override
        public String getDefaultDate()
        {
            return this.defaultDate;
        }
        @Override
        public ScriptingContainer getJRuby()
        {
            return null;
        }
    }

    private class TimestampParserColumnOptionImpl implements TimestampParser.TimestampColumnOption
    {
        private final Optional<DateTimeZone> timeZone;
        private final Optional<String> format;
        private final Optional<String> date;
        public TimestampParserColumnOptionImpl(
                Optional<DateTimeZone> timeZone,
                Optional<String> format,
                Optional<String> date)
        {
            this.timeZone = timeZone;
            this.format = format;
            this.date = date;
        }
        @Override
        public Optional<DateTimeZone> getTimeZone()
        {
            return this.timeZone;
        }
        @Override
        public Optional<String> getFormat()
        {
            return this.format;
        }
        @Override
        public Optional<String> getDate()
        {
            return this.date;
        }
    }

    // ToDo: Replace with `new TimestampParser(format, timezone)`
    // after deciding to drop supporting embulk < 0.8.29.
    private TimestampParser createTimestampParser(String format, DateTimeZone timezone)
    {
        return createTimestampParser(format, timezone, "1970-01-01");
    }

    // ToDo: Replace with `new TimestampParser(format, timezone, date)`
    // after deciding to drop supporting embulk < 0.8.29.
    private TimestampParser createTimestampParser(String format, DateTimeZone timezone, String date)
    {
        TimestampParserTaskImpl task = new TimestampParserTaskImpl(timezone, format, date);
        TimestampParserColumnOptionImpl columnOption = new TimestampParserColumnOptionImpl(
                Optional.of(timezone), Optional.of(format), Optional.of(date));
        return new TimestampParser(task, columnOption);
    }
}
