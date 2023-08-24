package org.embulk.filter.typecast;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;

import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

public class TypecastFilterPlugin implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(TypecastFilterPlugin.class);

    public TypecastFilterPlugin()
    {
    }

    // NOTE: This is not spi.ColumnConfig
    public interface ColumnConfig extends Task
    {
        @Config("name")
        String getName();

        @Config("type")
        Type getType();

        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZone();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        public Optional<String> getDate();
    }

    public interface PluginTask extends Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        List<ColumnConfig> getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        Boolean getStopOnInvalidRecord();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZone();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        public String getDefaultTimestampFormat();

        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        public String getDefaultDate();
    }

    @Override
    public void transaction(final ConfigSource config, final Schema inputSchema,
            final FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        configure(task, inputSchema);
        Schema outputSchema = buildOuputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    private void configure(final PluginTask task, final Schema inputSchema)
    {
        List<ColumnConfig> columnConfigs = task.getColumns();
        // throw if column does not exist
        for (ColumnConfig columnConfig : columnConfigs) {
            String name = columnConfig.getName();
            if (PathCompiler.isProbablyJsonPath(name)) {
                // check only top level column name
                String columnName = JsonPathUtil.getColumnName(name);
                inputSchema.lookupColumn(columnName);
            }
            else {
                inputSchema.lookupColumn(name);
            }
        }
        // throw if timestamp is specified in json path
        for (ColumnConfig columnConfig : columnConfigs) {
            String name = columnConfig.getName();
            if (PathCompiler.isProbablyJsonPath(name) && columnConfig.getType() instanceof TimestampType) {
                throw new ConfigException(String.format("embulk-filter-typecast: timestamp type is not supported in json column: \"%s\"", name));
            }
        }
    }

    private Schema buildOuputSchema(final PluginTask task, final Schema inputSchema)
    {
        List<ColumnConfig> columnConfigs = task.getColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (Column inputColumn : inputSchema.getColumns()) {
            String name = inputColumn.getName();
            Type   type = inputColumn.getType();
            ColumnConfig columnConfig = getColumnConfig(name, columnConfigs);
            if (columnConfig != null) {
                type = columnConfig.getType();
            }
            Column outputColumn = new Column(i++, name, type);
            builder.add(outputColumn);
        }
        return new Schema(builder.build());
    }

    private ColumnConfig getColumnConfig(String name, List<ColumnConfig> columnConfigs)
    {
        // hash should be faster, though
        for (ColumnConfig columnConfig : columnConfigs) {
            if (columnConfig.getName().equals(name)) {
                return columnConfig;
            }
        }
        return null;
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    inputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
