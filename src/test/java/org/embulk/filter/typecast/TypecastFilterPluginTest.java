package org.embulk.filter.typecast;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.DataException;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TypecastFilterPluginTest
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final CsvMapper CSV_MAPPER = new CsvMapper();

    @Rule
    public TestingEmbulk embulk =
            TestingEmbulk.builder()
                    .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
                    .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
                    .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
                    .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
                    .registerPlugin(FilterPlugin.class, "typecast", TypecastFilterPlugin.class)
                    .build();

    @Before
    public void setUp()
    {
        embulk.reset();
    }

    private ConfigSource newConfig()
    {
        return embulk.newConfig();
    }

    private Map<String, String> inputColumn(String name, String type) {
        return inputColumn(name, type, null);
    }

    private Map<String, String> inputColumn(String name, String type, String format) {
        Map<String, String> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        if (format != null) {
            column.put("format", format);
        }

        return column;
    }

    private List<Map<String, String>> buildInputColumnConfigs() {
        List<Map<String, String>> columns = new ArrayList<>();
        columns.add(inputColumn("string_value", "string"));
        columns.add(inputColumn("long_value", "long"));
        columns.add(inputColumn("double_value", "double"));
        columns.add(inputColumn("ts", "timestamp", "%Y-%m-%dT%H:%M:%SZ"));
        columns.add(inputColumn("json_value", "json"));

        return columns;
    }

    private ConfigSource getInputConfigSource(String resourceName, List<Map<String, String>> inputColumnConfigs)
    {
        ConfigSource input = newConfig();

        input.set("type", "file")
                .set("path_prefix", ClassLoader.getSystemResource(resourceName).getPath())
                .set("file_ext", "csv")
                .set("parser", newConfig()
                        .set("type", "csv")
                        .set("header_line", true)
                        .set("newline", "LF")
                        .set("columns", inputColumnConfigs));
        return input;
    }


    @Test
    public void testTypeCastNoColumn() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("hoge", row.get(0));
        assertEquals("12345", row.get(1));
        assertEquals("98765.4321", row.get(2));
        assertEquals("2023-08-24 11:43:01.000000 +0000", row.get(3));
        assertEquals("{\"key1\":9999,\"key2\":\"foo\"}", row.get(4));
    }

    @Test
    public void testTypeCastSimple() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        List<Map<String, String>> outputColumns = new ArrayList<>();
        outputColumns.add(inputColumn("long_value", "double"));
        outputColumns.add(inputColumn("double_value", "long"));
        filter.set("columns", outputColumns);

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("12345.0", row.get(1));
        assertEquals("98765", row.get(2));
    }

    @Test
    public void testTypeCastTimestamp() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        List<Map<String, String>> outputColumns = new ArrayList<>();
        outputColumns.add(inputColumn("long_value", "timestamp", "%s"));
        outputColumns.add(inputColumn("ts", "long", "%s"));
        filter.set("columns", outputColumns);

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("1970-01-01 03:25:45.000000 +0000", row.get(1));
        assertEquals("1692877381", row.get(3));
    }

    @Test
    public void testTypeCastJsonToString() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        List<Map<String, String>> outputColumns = new ArrayList<>();
        outputColumns.add(inputColumn("json_value", "string"));
        filter.set("columns", outputColumns);

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("{\"key1\":9999,\"key2\":\"foo\"}", row.get(4));
    }

    @Test
    public void testTypeCastJsonPath() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("json_value", "string");
        jsonColumn.put("json_path", "$.key1");
        filter.set("columns", Collections.singletonList(jsonColumn));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("9999", row.get(4));
    }

    @Test
    public void testTypeCastJsonPathWithNoResults() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("json_value", "string");
        jsonColumn.put("json_path", "$.no_key");
        filter.set("columns", Collections.singletonList(jsonColumn));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("", row.get(4));
    }

    @Test
    public void testStopOnInvalidRecord() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("json_value", "string");
        jsonColumn.put("json_path", "$.no_key");
        filter.set("columns", Collections.singletonList(jsonColumn));
        filter.set("stop_on_invalid_record", true);

        assertThrows(PartialExecutionException.class, () -> {
            embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        });
    }

    @Test
    public void testTypeCastComplexJson() throws IOException
    {
        ConfigSource input = getInputConfigSource("data_complex.csv", Collections.singletonList(inputColumn("complex_json", "json")));
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("complex_json", "string");
        jsonColumn.put("json_path", "$.a[0].b");
        filter.set("columns", Collections.singletonList(jsonColumn));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("123", row.get(0));
    }

    @Test
    public void testTypeCastComplexJsonExtractNestedJson() throws IOException
    {
        ConfigSource input = getInputConfigSource("data_complex.csv", Collections.singletonList(inputColumn("complex_json", "json")));
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("complex_json", "json");
        jsonColumn.put("json_path", "$.d");
        filter.set("columns", Collections.singletonList(jsonColumn));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertEquals("{\"e\":\"e-value\",\"f\":\"f-value\"}", row.get(0));
    }
}