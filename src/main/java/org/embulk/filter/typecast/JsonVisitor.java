package org.embulk.filter.typecast;

import io.github.medjed.jsonpathcompiler.expressions.Path;
import io.github.medjed.jsonpathcompiler.expressions.path.ArrayPathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PathCompiler;
import io.github.medjed.jsonpathcompiler.expressions.path.PathToken;
import io.github.medjed.jsonpathcompiler.expressions.path.PropertyPathToken;
import org.embulk.filter.typecast.TypecastFilterPlugin.ColumnConfig;
import org.embulk.filter.typecast.TypecastFilterPlugin.PluginTask;

import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JsonVisitor
{
    private static final Logger logger = Exec.getLogger(TypecastFilterPlugin.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final HashSet<String> shouldVisitSet = new HashSet<>();
    private final HashMap<String, Type> jsonPathTypeMap = new HashMap<>();
    private final JsonCaster jsonCaster = new JsonCaster();

    JsonVisitor(PluginTask task, Schema inputSchema, Schema outputSchema)
    {
        this.task         = task;
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;

        assertJsonPathFromat();
        buildShouldVisitSet();
        buildJsonPathTypeMap();
    }

    private void assertJsonPathFromat()
    {
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            JsonPathUtil.assertJsonPathFormat(name);
        }
    }

    private void buildJsonPathTypeMap()
    {
        // json path => Type
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            Path compiledPath = PathCompiler.compile(name);
            Type type = columnConfig.getType();
            this.jsonPathTypeMap.put(compiledPath.toString(), type);
        }
    }

    private void buildShouldVisitSet()
    {
        // json partial path => Boolean to avoid unnecessary type: json visit
        for (ColumnConfig columnConfig : task.getColumns()) {
            String name = columnConfig.getName();
            if (! PathCompiler.isProbablyJsonPath(name)) {
                continue;
            }
            PathToken parts = PathCompiler.compile(name).getRoot();
            StringBuilder partialPath = new StringBuilder("$");
            while (! parts.isLeaf()) {
                parts = parts.next(); // first next() skips "$"
                partialPath.append(parts.getPathFragment());
                this.shouldVisitSet.add(partialPath.toString());
            }
        }
    }

    private boolean shouldVisit(String jsonPath)
    {
        return shouldVisitSet.contains(jsonPath);
    }

    public Value visit(String rootPath, Value value)
    {
        if (!shouldVisit(rootPath)) {
            return value;
        }
        Type outputType = jsonPathTypeMap.get(rootPath);
        if (outputType != null) {
            if (value.isBooleanValue()) {
                return jsonCaster.fromBoolean(outputType, value.asBooleanValue());
            }
            else if (value.isIntegerValue()) {
                return jsonCaster.fromLong(outputType, value.asIntegerValue());
            }
            else if (value.isFloatValue()) {
                return jsonCaster.fromDouble(outputType, value.asFloatValue());
            }
            else if (value.isStringValue()) {
                return jsonCaster.fromString(outputType, value.asStringValue());
            }
            else if (value.isArrayValue()) {
                return jsonCaster.fromJson(outputType, value);
            }
            else if (value.isMapValue()) {
                return jsonCaster.fromJson(outputType, value);
            }
            else {
                return value;
            }
        }
        if (value.isArrayValue()) {
            ArrayValue arrayValue = value.asArrayValue();
            int size = arrayValue.size();
            Value[] newValue = new Value[size];
            for (int i = 0; i < size; i++) {
                String pathFragment = ArrayPathToken.getPathFragment(i);
                String k = new StringBuilder(rootPath).append(pathFragment).toString();
                if (!shouldVisit(k)) {
                    k = new StringBuilder(rootPath).append("[*]").toString(); // try [*] too
                }
                Value v = arrayValue.get(i);
                newValue[i] = visit(k, v);
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
                String pathFragment = PropertyPathToken.getPathFragment(k.asStringValue().asString());
                String newPath = new StringBuilder(rootPath).append(pathFragment).toString();
                Value r = visit(newPath, v);
                newValue[i++] = k;
                newValue[i++] = r;
            }
            return ValueFactory.newMap(newValue, true);
        }
        else {
            return value;
        }
    }
}
