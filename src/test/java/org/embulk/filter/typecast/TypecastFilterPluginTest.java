package org.embulk.filter.typecast;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.jayway.jsonpath.matchers.JsonPathMatchers;
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
import org.embulk.util.json.JsonParser;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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
        assertEquals("{\"key1\":9999,\"key2\":\"true\"}", row.get(4));
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
        assertEquals("{\"key1\":9999,\"key2\":\"true\"}", row.get(4));
    }

    @Test
    public void testTypeCastJsonPath() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn1 = inputColumn("$.json_value.key1", "string");
        Map<String, String> jsonColumn2 = inputColumn("$.json_value.key2", "boolean");
        filter.set("columns", Arrays.asList(jsonColumn1, jsonColumn2));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertThat(row.get(4), hasJsonPath("$.key1", equalTo("9999")));
        assertThat(row.get(4), hasJsonPath("$.key2", equalTo(true)));
    }

    @Test
    public void testTypeCastJsonPathWithNoResults() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("$.json_value.no_key", "string");
        filter.set("columns", Collections.singletonList(jsonColumn));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertThat(row.get(4), hasJsonPath("$.key1", equalTo(9999)));
        assertThat(row.get(4), hasJsonPath("$.key2", equalTo("true")));
    }

    @Test
    public void testStopOnInvalidRecord() throws IOException
    {
        ConfigSource input = getInputConfigSource("data.csv", buildInputColumnConfigs());
        Path tempDir = Files.createTempDirectory("embulk-filter-typecast-testing");
        Path outputFile = tempDir.resolve("output.csv");

        ConfigSource filter = newConfig();
        filter.set("type", "typecast");

        Map<String, String> jsonColumn = inputColumn("$.json_value.no_key", "string");
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

        Map<String, String> jsonColumn1 = inputColumn("$.complex_json.a[0].b", "string");
        Map<String, String> jsonColumn2 = inputColumn("$.complex_json.a[0].c", "double");
        filter.set("columns", Arrays.asList(jsonColumn1, jsonColumn2));

        embulk.inputBuilder().in(input).outputPath(outputFile).filters(Collections.singletonList(filter)).run();
        byte[] result = Files.readAllBytes(outputFile);
        List<String> row = CSV_MAPPER.readValue(result, new TypeReference<List<String>>() {});
        assertThat(row.get(0), hasJsonPath("$.a[0].b", equalTo("123")));
        assertThat(row.get(0), hasJsonPath("$.a[0].c", equalTo(456.0)));
    }
}