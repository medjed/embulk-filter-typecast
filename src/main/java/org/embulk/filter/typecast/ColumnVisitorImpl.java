package org.embulk.filter.typecast;

import org.embulk.spi.*;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;

import org.embulk.filter.typecast.TypecastFilterPlugin.ColumnConfig;
import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;

import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.HashMap;

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
    private final TypecastPageBuilder typecastPageBuilder;

    ColumnVisitorImpl(PluginTask task, Schema inputSchema, Schema outputSchema,
            PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader   = pageReader;
        this.pageBuilder  = pageBuilder;

        this.typecastPageBuilder = new TypecastPageBuilder(task, inputSchema, outputSchema, pageReader, pageBuilder);

        buildOutputColumnMap();
        buildTimestampParserMap();
        buildTimestampFormatterMap();
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

    private TimestampParser getTimestampParser(ColumnConfig columnConfig, PluginTask task)
    {
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        return new TimestampParser(task.getJRuby(), format, timezone);
    }

    private TimestampFormatter getTimestampFormatter(ColumnConfig columnConfig, PluginTask task)
    {
        String format = columnConfig.getFormat().or(task.getDefaultTimestampFormat());
        DateTimeZone timezone = columnConfig.getTimeZone().or(task.getDefaultTimeZone());
        return new TimestampFormatter(task.getJRuby(), format, timezone);
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
                typecastPageBuilder.setFromBoolean(outputColumn, pageReader.getBoolean(inputColumn));
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
                typecastPageBuilder.setFromLong(outputColumn, pageReader.getLong(inputColumn));
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
                typecastPageBuilder.setFromDouble(outputColumn, pageReader.getDouble(inputColumn));
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
                typecastPageBuilder.setFromString(outputColumn, pageReader.getString(inputColumn), timestampParser);
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
                typecastPageBuilder.setFromTimestamp(outputColumn, pageReader.getTimestamp(inputColumn), timestampFormatter);
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
                typecastPageBuilder.setFromJson(outputColumn, pageReader.getJson(inputColumn));
            }
        };
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }
}
