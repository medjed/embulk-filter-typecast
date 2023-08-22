package org.embulk.filter.typecast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;
import org.embulk.filter.typecast.TypecastFilterPlugin.TypecastColumnConfig;
import org.embulk.filter.typecast.cast.BooleanCast;
import org.embulk.filter.typecast.cast.DoubleCast;
import org.embulk.filter.typecast.cast.JsonCast;
import org.embulk.filter.typecast.cast.LongCast;
import org.embulk.filter.typecast.cast.StringCast;
import org.embulk.filter.typecast.cast.TimestampCast;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

import java.util.HashMap;

class ColumnCaster
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final PluginTask task;
    private final ConfigMapper configMapper;
    private final Schema inputSchema;
    private final PageBuilder pageBuilder;
    private final HashMap<String, TimestampFormatter> timestampFormatterMap = new HashMap<>();
    private final HashMap<String, JsonPath> jsonPathMap = new HashMap<>();

    ColumnCaster(PluginTask task, ConfigMapper configMapper, Schema inputSchema,
                 PageBuilder pageBuilder)
    {
        this.task = task;
        this.configMapper = configMapper;
        this.inputSchema = inputSchema;
        this.pageBuilder = pageBuilder;

        buildColumnConfigMap();
    }

    private void buildColumnConfigMap()
    {
        // columnName => TimestampFormatter
        for (ColumnConfig columnConfig : task.getColumns().getColumns()) {
            TypecastColumnConfig typecastColumnConfig = configMapper.map(columnConfig.getOption(), TypecastColumnConfig.class);
            typecastColumnConfig.getJsonPath().ifPresent(jsonPath -> jsonPathMap.put(columnConfig.getName(), JsonPath.compile(jsonPath)));
            Column inputColumn = inputSchema.lookupColumn(columnConfig.getName());
            if (inputColumn.getType() instanceof TimestampType && columnConfig.getType() instanceof StringType) {
                TimestampFormatter formatter = createTimestampFormatter(task, typecastColumnConfig);
                this.timestampFormatterMap.put(columnConfig.getName(), formatter);
            }
        }
    }

    public void setFromBoolean(Column outputColumn, boolean value)
    {
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
            assert false;
        }
    }

    public void setFromDouble(Column outputColumn, double value)
    {
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
            assert false;
        }
    }

    public void setFromString(Column outputColumn, String value)
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
            TimestampFormatter timestampParser = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setTimestamp(outputColumn, StringCast.asTimestamp(value, timestampParser));
        } else if (outputType instanceof JsonType) {
            Value jsonValue = StringCast.asJson(value);
            JsonPath jsonPath = jsonPathMap.get(outputColumn.getName());
            if (jsonPath != null) {
                try {
                    Object rawValue = jsonPath.read(value);
                    String partialJson = OBJECT_MAPPER.writeValueAsString(rawValue);
                    JsonParser jsonParser = new JsonParser();
                    pageBuilder.setJson(outputColumn, jsonParser.parse(partialJson));
                } catch (JsonProcessingException | PathNotFoundException e) {
                    throw new DataException(e);
                }
            } else {
                pageBuilder.setJson(outputColumn, jsonValue);
            }
        } else {
            assert false;
        }
    }

    public void setFromTimestamp(Column outputColumn, Timestamp value)
    {
        Type outputType = outputColumn.getType();
        if (outputType instanceof BooleanType) {
            pageBuilder.setBoolean(outputColumn, TimestampCast.asBoolean(value));
        } else if (outputType instanceof LongType) {
            pageBuilder.setLong(outputColumn, TimestampCast.asLong(value));
        } else if (outputType instanceof DoubleType) {
            pageBuilder.setDouble(outputColumn, TimestampCast.asDouble(value));
        } else if (outputType instanceof StringType) {
            TimestampFormatter timestampFormatter = timestampFormatterMap.get(outputColumn.getName());
            pageBuilder.setString(outputColumn, TimestampCast.asString(value, timestampFormatter));
        } else if (outputType instanceof TimestampType) {
            pageBuilder.setTimestamp(outputColumn, TimestampCast.asTimestamp(value));
        } else if (outputType instanceof JsonType) {
            pageBuilder.setJson(outputColumn, TimestampCast.asJson(value));
        } else {
            assert false;
        }
    }

    public void setFromJson(Column outputColumn, Value value)
    {
        JsonPath jsonPath = jsonPathMap.get(outputColumn.getName());
        Value castedValue;
        if (jsonPath != null) {
            String partialJson;
            try {
                Object rawValue = jsonPath.read(value.toJson());
                partialJson = OBJECT_MAPPER.writeValueAsString(rawValue);
            } catch (JsonProcessingException | PathNotFoundException e) {
                throw new DataException(e);
            }
            JsonParser jsonParser = new JsonParser();
            castedValue = jsonParser.parse(partialJson);
        } else {
            castedValue = value;
        }
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
            assert false;
        }
    }

    private TimestampFormatter createTimestampFormatter(PluginTask task, TypecastColumnConfig columnConfig)
    {
        return TimestampFormatter.builder(columnConfig.getFormat().orElse(task.getDefaultTimestampFormat()), true)
                .setDefaultZoneFromString(columnConfig.getTimeZone().orElse(task.getDefaultTimeZone()))
                .setDefaultDateFromString(columnConfig.getDate().orElse(task.getDefaultDate())).build();
    }
}
