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
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TypecastFilterPlugin implements FilterPlugin
{
    private final ConfigMapperFactory configMapperFactory = ConfigMapperFactory.withDefault();

    // NOTE: This is not spi.ColumnConfig
    public interface TypecastColumnConfig extends Task
    {
        @Config("json_path")
        @ConfigDefault("null")
        Optional<String> getJsonPath();

        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZone();

        @Config("format")
        @ConfigDefault("null")
        Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
    }

    public interface PluginTask extends Task
    {
        @Config("columns")
        @ConfigDefault("[]")
        SchemaConfig getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

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
        ConfigMapper configMapper = configMapperFactory.createConfigMapper();
        PluginTask task = configMapper.map(config, PluginTask.class);
        SchemaConfig schemaConfig = task.getColumns();

        configure(inputSchema, schemaConfig);
        Schema outputSchema = buildOutputSchema(inputSchema, schemaConfig);
        control.run(task.toTaskSource(), outputSchema);
    }

    private void configure(final Schema inputSchema, SchemaConfig schemaConfig)
    {
        // throw if column does not exist
        for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
            String name = columnConfig.getName();
            inputSchema.lookupColumn(name);
        }
        // throw if timestamp is specified in json path
        for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
            String name = columnConfig.getName();
            if (JsonPathUtil.isProbablyJsonPath(name) && columnConfig.getType() instanceof TimestampType) {
                throw new ConfigException(String.format("embulk-filter-typecast: timestamp type is not supported in json column: \"%s\"", name));
            }
        }
    }

    private Schema buildOutputSchema(Schema inputSchema, SchemaConfig schemaConfig)
    {
        List<Column> outputColumns = new ArrayList<>();
        List<ColumnConfig> columnConfigs = schemaConfig.getColumns();
        int i = 0;
        for (Column inputColumn : inputSchema.getColumns()) {
            String name = inputColumn.getName();
            Optional<ColumnConfig> typecastedColumn = columnConfigs.stream().filter(columnConfig -> columnConfig.getName().equals(name)).findFirst();
            Type type;
            if (typecastedColumn.isPresent()) {
                type = typecastedColumn.get().getType();
            } else {
                type = inputColumn.getType();
            }
            Column outputColumn = new Column(i++, name, type);
            outputColumns.add(outputColumn);
        }
        return new Schema(outputColumns);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        ConfigMapper configMapper = configMapperFactory.createConfigMapper();
        TaskMapper taskMapper = configMapperFactory.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, configMapper, inputSchema, outputSchema, pageReader, pageBuilder);

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
