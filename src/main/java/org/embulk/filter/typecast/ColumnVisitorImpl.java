package org.embulk.filter.typecast;

import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

class ColumnVisitorImpl implements ColumnVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnVisitorImpl.class);

    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, Column> outputColumnMap = new HashMap<>();
    private final ColumnCaster columnCaster;

    ColumnVisitorImpl(PluginTask task, ConfigMapper configMapper, Schema inputSchema, Schema outputSchema,
                      PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader   = pageReader;
        this.pageBuilder  = pageBuilder;

        this.columnCaster = new ColumnCaster(task, configMapper, inputSchema, pageBuilder);

        buildOutputColumnMap();
    }

    private void buildOutputColumnMap()
    {
        for (Column column : inputSchema.getColumns()) {
            this.outputColumnMap.put(column.getName(), column);
        }
        // override typecasted column
        for (Column column : outputSchema.getColumns()) {
            this.outputColumnMap.put(column.getName(), column);
        }
    }

    private interface PageBuildable
    {
        void run() throws DataException;
    }

    private void withStopOnInvalidRecord(final PageBuildable op, final Column inputColumn, final Column outputColumn)
            throws DataException
    {
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            if (task.getStopOnInvalidRecord()) {
                op.run();
            }
            else {
                try {
                    op.run();
                }
                catch (final DataException ex) {
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
        PageBuildable op = () -> columnCaster.setFromBoolean(outputColumn, pageReader.getBoolean(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void longColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = () -> columnCaster.setFromLong(outputColumn, pageReader.getLong(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void doubleColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = () -> columnCaster.setFromDouble(outputColumn, pageReader.getDouble(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void stringColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = () -> columnCaster.setFromString(outputColumn, pageReader.getString(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void timestampColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = () -> columnCaster.setFromTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }

    @Override
    public void jsonColumn(final Column inputColumn)
    {
        final Column outputColumn = outputColumnMap.get(inputColumn.getName());
        PageBuildable op = () -> columnCaster.setFromJson(outputColumn, pageReader.getJson(inputColumn));
        withStopOnInvalidRecord(op, inputColumn, outputColumn);
    }
}
