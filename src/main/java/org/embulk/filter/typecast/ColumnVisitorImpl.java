package org.embulk.filter.typecast;

import com.google.common.base.Throwables;
import org.embulk.spi.*;
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
import java.util.List;

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
    private final HashSet<String> shouldVisitRecursivelySet = new HashSet<>();

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
        buildShouldVisitRecursivelySet();;
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
        // columnName or jsonPath => TimestampParser
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampParser parser = getTimestampParser(columnConfig, task);
            this.timestampParserMap.put(columnConfig.getName(), parser); // NOTE: value would be null
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
        // columnName or jsonPath => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns()) {
            TimestampFormatter parser = getTimestampFormatter(columnConfig, task);
            this.timestampFormatterMap.put(columnConfig.getName(), parser); // NOTE: value would be null
        }
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        return new TimestampFormatter(task.getJRuby(), format, timezone);
    }

    private void buildShouldVisitRecursivelySet()
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
                    this.shouldVisitRecursivelySet.add(partialPath.toString());
                    for (int j = 1; j < arrayParts.length; j++) {
                        partialPath.append("[").append(arrayParts[j]);
                        this.shouldVisitRecursivelySet.add(partialPath.toString());
                    }
                }
                else {
                    partialPath.append(".").append(parts[i]);
                    this.shouldVisitRecursivelySet.add(partialPath.toString());
                }
            }
        }
    }

    private boolean shouldVisitRecursively(String name)
    {
        return shouldVisitRecursivelySet.contains(name);
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
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = new PageBuildable() {
            public void run() throws DataException {
                TypecastPageBuilder.setFromJson(pageBuilder, outputColumn, pageReader.getJson(inputColumn));
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

}
