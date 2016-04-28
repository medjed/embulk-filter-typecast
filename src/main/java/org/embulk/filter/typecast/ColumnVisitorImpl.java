package org.embulk.filter.typecast;

import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import org.embulk.filter.typecast.TypecastFilterPlugin.ColumnConfig;
import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;

import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ColumnVisitorImpl
        implements ColumnVisitor
{
    private static final Logger logger = Exec.getLogger(TypecastFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, Column> outputColumnMap = new HashMap<>();
    private final HashMap<String, TimestampParser> timestampParserMap = new HashMap<>();
    private final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<>();
    private final HashSet<String> shouldVisitJsonPathSet = new HashSet<>();
    private final HashMap<String, Type> jsonPathTypeMap = new HashMap<>();

    ColumnVisitorImpl(PluginTask task, Schema inputSchema, Schema outputSchema,
                      PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader   = pageReader;
        this.pageBuilder  = pageBuilder;

        buildOutputColumnMap();
        buildTimestampParserMap();
        buildTimestampFormatterMap();
        buildShouldVisitJsonPathSet();;
        buildJsonPathTypeMap();
    }

    private void buildOutputColumnMap()
    {
        // columnName => outputColumn
        for (Column column : outputSchema.getColumns()) {
            this.outputColumnMap.put(column.getName(), column);
        }
    }

    private void buildTimestampParserMap()
    {
        // columnName => TimestampParser
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (columnConfig.getName().startsWith("$.")) {
                continue; // type: json columns do not support type: timestamp
            }
            Column inputColumn = inputSchema.lookupColumn(columnConfig.getName());
            if (inputColumn.getType() instanceof StringType && columnConfig.getType() instanceof TimestampType) {
                TimestampParser parser = getTimestampParser(columnConfig, task);
                this.timestampParserMap.put(columnConfig.getName(), parser);
            }
        }
    }

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        return new TimestampParser(task.getJRuby(), format, timezone);
    }

    private void buildTimestampFormatterMap()
    {
        // columnName => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns()) {
            if (columnConfig.getName().startsWith("$.")) {
                continue; // type: json columns do not have type: timestamp
            }
            Column inputColumn = inputSchema.lookupColumn(columnConfig.getName());
            if (inputColumn.getType() instanceof TimestampType && columnConfig.getType() instanceof StringType) {
                TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
                this.timestampFormatterMap.put(columnConfig.getName(), parser);
            }
        }
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        return new TimestampFormatter(task.getJRuby(), format, timezone);
    }

    private void buildShouldVisitJsonPathSet()
    {
        // json partial path => Boolean to avoid unnecessary type: json visit
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (!name.startsWith("$.")) {
                continue;
            }
            String[] parts = name.split("\\.");
            StringBuilder partialPath = new StringBuilder("$");
            for (int i = 1; i < parts.length; i++) {
                if (parts[i].contains("[")) {
                    String[] arrayParts = parts[i].split("\\[");
                    partialPath.append(".").append(arrayParts[0]);
                    this.shouldVisitJsonPathSet.add(partialPath.toString());
                    for (int j = 1; j < arrayParts.length; j++) {
                        partialPath.append("[").append(arrayParts[j]);
                        this.shouldVisitJsonPathSet.add(partialPath.toString());
                    }
                }
                else {
                    partialPath.append(".").append(parts[i]);
                    this.shouldVisitJsonPathSet.add(partialPath.toString());
                }
            }
        }
    }

    private void buildJsonPathTypeMap()
    {
        // json path => Type
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (!name.startsWith("$.")) {
                continue;
            }
            Type type = columnConfig.getType();
            this.jsonPathTypeMap.put(name, type);
        }
    }

    private boolean shouldVisitJsonPath(String jsonPath)
    {
        return shouldVisitJsonPathSet.contains(jsonPath);
    }

    private Value castJsonRecursively(PluginTask task, String jsonPath, Value value)
    {
        if (!shouldVisitJsonPath(jsonPath)) {
            return value;
        }
        if (value.isArrayValue()) {
            ArrayValue arrayValue = value.asArrayValue();
            int size = arrayValue.size();
            Value[] newValue = new Value[size];
            for (int i = 0; i < size; i++) {
                String k = new StringBuilder(jsonPath).append("[").append(Integer.toString(i)).append("]").toString();
                Value v = arrayValue.get(i);
                newValue[i] = castJsonRecursively(task, k, v);
            }
            return ValueFactory.newArray(newValue, true);
        }
        else if (value.isMapValue()) {
            MapValue mapValue = value.asMapValue();
            int size = mapValue.size() * 2;
            Value[] newValue = new Value[size];
            int i = 0;
            for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
                Value k = entry.getKey();
                Value v = entry.getValue();
                String newPath = new StringBuilder(jsonPath).append(".").append(k.asStringValue().asString()).toString();
                Value r = castJsonRecursively(task, newPath, v);
                newValue[i++] = k;
                newValue[i++] = r;
            }
            return ValueFactory.newMap(newValue, true);
        }
        else if (value.isBooleanValue()) {
            Type outputType = jsonPathTypeMap.get(jsonPath);
            return TypecastJsonBuilder.getFromBoolean(outputType, value.asBooleanValue().getBoolean());
        }
        else if (value.isIntegerValue()) {
            Type outputType = jsonPathTypeMap.get(jsonPath);
            return TypecastJsonBuilder.getFromLong(outputType, value.asIntegerValue().asLong());
        }
        else if (value.isFloatValue()) {
            Type outputType = jsonPathTypeMap.get(jsonPath);
            return TypecastJsonBuilder.getFromDouble(outputType, value.asFloatValue().toDouble());
        }
        else if (value.isStringValue()) {
            Type outputType = jsonPathTypeMap.get(jsonPath);
            return TypecastJsonBuilder.getFromString(outputType, value.asStringValue().asString());
        }
        else {
            return value;
        }
    }

    private interface PageBuildable
    {
        public void run() throws DataException;
    }

    private void withStopOnInvalidRecord(final PageBuildable op, final Column inputColumn, final Column outputColumn) throws DataException {
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            if (task.getStopOnInvalidRecord()) {
                op.run();
            } else {
                try {
                    op.run();
                } catch (final DataException ex) {
                    logger.warn(ex.getMessage());
                    pageBuilder.setNull(outputColumn);
                }
            }
        }
    }

    @Override
    public void booleanColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromBoolean(pageBuilder, outputColumn, pageReader.getBoolean(inputColumn));
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void longColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromLong(pageBuilder, outputColumn, pageReader.getLong(inputColumn));
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void doubleColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromDouble(pageBuilder, outputColumn, pageReader.getDouble(inputColumn));
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void stringColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        final TimestampParser timestampParser = timestampParserMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromString(
                        pageBuilder, outputColumn, pageReader.getString(inputColumn), timestampParser);
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void timestampColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        final TimestampFormatter timestampFormatter = timestampFormatterMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromTimestamp(
                        pageBuilder, outputColumn, pageReader.getTimestamp(inputColumn), timestampFormatter);
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void jsonColumn(final Column inputColumn)
    {
        String jsonPath = new StringBuilder("$.").append(inputColumn.getName()).toString();
        Value value = pageReader.getJson(inputColumn);
        final Value castedValue = castJsonRecursively(task, jsonPath, value);
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromJson(pageBuilder, outputColumn, castedValue);
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }
}
