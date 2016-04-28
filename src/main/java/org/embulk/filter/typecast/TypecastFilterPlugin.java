package org.embulk.filter.typecast;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;

import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.util.List;

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
        public Optional<DateTimeZone> getTimeZone();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();
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
        public DateTimeZone getDefaultTimeZone();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        public String getDefaultTimestampFormat();

        @ConfigInject
        ScriptingContainer getJRuby();
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
            if (name.startsWith("$.")) { // check only top level column name
                String firstName = name.split("\\.", 3)[1];
                inputSchema.lookupColumn(firstName);
            }
            else {
                inputSchema.lookupColumn(name);
            }
        }
        // throw if timestamp is specified in json path
        for (ColumnConfig columnConfig : columnConfigs) {
            String name = columnConfig.getName();
            if (name.startsWith("$.") && columnConfig.getType() instanceof TimestampType) {
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
