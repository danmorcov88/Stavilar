package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.ATTR_QUERY_ID;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.ATTR_ROWS_READ;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.MAX_ROWS_PER_FLOWFILE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.RECORD_WRITER;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_ORIGINAL;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.SQL_QUERY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class QueryClickHouseRecordIT {

    private static final String IMAGE = System.getProperty("clickhouse.image", "clickhouse/clickhouse-server:26.8");
    private static final String USER = "stavilar";
    private static final String DB = "stavilar";
    private static final int FIVE_MILLION = 5_000_000;

    @Container
    private static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withEnv("CLICKHOUSE_USER", USER)
            .withEnv("CLICKHOUSE_PASSWORD", USER)
            .withEnv("CLICKHOUSE_DB", DB)
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    private static Client admin;

    private TestRunner runner;
    private StandardClickHouseConnectionService service;

    @BeforeAll
    static void createTables() {
        admin = new Client.Builder()
                .addEndpoint("http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123))
                .setUsername(USER).setPassword(USER).setDefaultDatabase(DB)
                .build();
        execute("CREATE TABLE types ("
                + "i8 Int8, u16 UInt16, u32 UInt32, i64 Int64, u64 UInt64, i128 Int128, f32 Float32, f64 Float64, dec Decimal(12, 3), "
                + "s String, fs FixedString(3), b Bool, u UUID, d Date, d32 Date32, dt DateTime('UTC'), dt64 DateTime64(3, 'Europe/Bucharest'), "
                + "e Enum8('a' = 1, 'b' = 2), n Nullable(String), lc LowCardinality(String), arr Array(Int32), arrs Array(Array(String)), "
                + "m Map(String, Int32), t Tuple(Int32, String), ip IPv4"
                + ") ENGINE = MergeTree ORDER BY i64");
        // Int128 min as a string: a bare numeric literal that large overflows while the server parses it
        execute("INSERT INTO types SELECT -5, 60000, 4000000000, -9000000000, 18446744073709551615, toInt128('-170141183460469231731687303715884105728'), 1.5, 2.25, 12345.678, "
                + "'Stăvilar', 'ab', true, '123e4567-e89b-12d3-a456-426614174000', '2024-03-15', '1950-01-02', '2024-03-15 11:45:10', '2024-03-15 13:45:10.123', "
                + "'b', NULL, 'low', [1, 2, 3], [['x'], []], map('k', 7), tuple(1, 'a'), '1.2.3.4'");
        execute("CREATE TABLE big (id UInt64, name String) ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO big SELECT number, concat('n', toString(number % 1000)) FROM numbers(" + FIVE_MILLION + ")");
    }

    @AfterAll
    static void cleanUp() {
        admin.close();
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(QueryClickHouseRecord.class);
        service = new StandardClickHouseConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.setProperty(service, StandardClickHouseConnectionService.ENDPOINTS, CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123));
        runner.setProperty(service, StandardClickHouseConnectionService.USERNAME, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.PASSWORD, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.DATABASE, DB);
        runner.enableControllerService(service);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
        final AvroRecordSetWriter writer = new AvroRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(RECORD_WRITER, "writer");
        runner.setIncomingConnection(false);
    }

    @Test
    void allTypesThroughAvroAndBack() throws Exception {
        runner.setProperty(SQL_QUERY, "SELECT * FROM types");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "1");
        out.assertAttributeEquals(ATTR_ROWS_READ, "1");
        out.assertAttributeEquals("mime.type", "application/avro-binary");
        assertTrue(out.getAttribute(ATTR_QUERY_ID).length() > 20);

        final Record row = readAvro(out).get(0);
        assertEquals(-5, row.getAsInt("i8"));
        assertEquals(60000, row.getAsInt("u16"));
        assertEquals(4_000_000_000L, row.getAsLong("u32"));
        assertEquals(-9_000_000_000L, row.getAsLong("i64"));
        assertEquals(new BigInteger("18446744073709551615"), new BigInteger(row.getAsString("u64")));
        assertEquals(new BigInteger("-170141183460469231731687303715884105728"), new BigInteger(row.getAsString("i128")));
        assertEquals(1.5f, row.getAsFloat("f32"));
        assertEquals(2.25, row.getAsDouble("f64"));
        assertEquals(new BigDecimal("12345.678"), new BigDecimal(row.getAsString("dec")));
        assertEquals("Stăvilar", row.getAsString("s"));
        assertEquals("ab", row.getAsString("fs"));
        assertEquals(Boolean.TRUE, row.getAsBoolean("b"));
        assertEquals("123e4567-e89b-12d3-a456-426614174000", row.getAsString("u"));
        assertEquals("2024-03-15", row.getAsLocalDate("d", null).toString());
        assertEquals("1950-01-02", row.getAsLocalDate("d32", null).toString());
        assertEquals(Instant.parse("2024-03-15T11:45:10Z"), Instant.ofEpochMilli(((java.util.Date) row.getValue("dt")).getTime()));
        assertEquals(Instant.parse("2024-03-15T11:45:10.123Z"), Instant.ofEpochMilli(((java.util.Date) row.getValue("dt64")).getTime()));
        assertEquals("b", row.getAsString("e"));
        assertNull(row.getValue("n"));
        assertEquals("low", row.getAsString("lc"));
        assertArrayEquals(new Object[]{1, 2, 3}, row.getAsArray("arr"));
        final Object[] arrs = row.getAsArray("arrs");
        assertArrayEquals(new Object[]{"x"}, (Object[]) arrs[0]);
        assertArrayEquals(new Object[0], (Object[]) arrs[1]);
        assertEquals(7, ((Map<?, ?>) row.getValue("m")).get("k"));
        assertEquals(1, ((Record) row.getValue("t")).getAsInt("_1"));
        assertEquals("a", ((Record) row.getValue("t")).getAsString("_2"));
        assertEquals("1.2.3.4", row.getAsString("ip"));
    }

    @Test
    void emptyResultGivesOneEmptyFlowFile() throws Exception {
        runner.setProperty(SQL_QUERY, "SELECT * FROM types WHERE 1 = 0");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "0");
        out.assertAttributeEquals(ATTR_ROWS_READ, "0");
        assertEquals(0, readAvro(out).size());
    }

    @Test
    void splitsFiveMillionRowsIntoFragments() throws Exception {
        runner.setProperty(SQL_QUERY, "SELECT id, name FROM big ORDER BY id");
        runner.setProperty(MAX_ROWS_PER_FLOWFILE, "1000000");
        final Runtime rt = Runtime.getRuntime();
        final long maxHeap = rt.maxMemory();
        final long start = System.nanoTime();
        runner.run();
        final long millis = (System.nanoTime() - start) / 1_000_000;

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 5);
        final List<MockFlowFile> out = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final String fragmentId = out.get(0).getAttribute("fragment.identifier");
        long totalBytes = 0;
        for (int i = 0; i < 5; i++) {
            out.get(i).assertAttributeEquals("fragment.index", String.valueOf(i));
            out.get(i).assertAttributeEquals("fragment.count", "5");
            out.get(i).assertAttributeEquals("fragment.identifier", fragmentId);
            out.get(i).assertAttributeEquals("record.count", "1000000");
            out.get(i).assertAttributeEquals(ATTR_ROWS_READ, "1000000");
            totalBytes += out.get(i).getSize();
        }
        // spot-check the boundaries without deserializing 5M records
        final List<Record> last = readAvro(out.get(4));
        assertEquals(1_000_000, last.size());
        assertEquals(FIVE_MILLION - 1, last.get(last.size() - 1).getAsLong("id"));
        System.out.println("5M rows read into 5 FlowFiles (" + totalBytes / 1_000_000 + " MB Avro) in " + millis + " ms with max heap " + maxHeap / 1_000_000 + " MB");
    }

    @Test
    void sqlErrorGoesToFailureWithServerMessage() {
        runner.setProperty(SQL_QUERY, "SELECT * FROM does_not_exist");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        assertTrue(out.getAttribute(ATTR_ERROR).contains("does_not_exist"), out.getAttribute(ATTR_ERROR));
        assertTrue(out.getAttribute(ATTR_QUERY_ID).length() > 20);
    }

    @Test
    void queryFromIncomingFlowFileContent() throws Exception {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT i8, s FROM types", Map.of("marker", "x"));
        runner.run();
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        runner.assertTransferCount(REL_FAILURE, 0);
        final MockFlowFile result = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        result.assertAttributeEquals("marker", "x");
        assertEquals("Stăvilar", readAvro(result).get(0).getAsString("s"));
        final MockFlowFile original = runner.getFlowFilesForRelationship(REL_ORIGINAL).get(0);
        original.assertContentEquals("SELECT i8, s FROM types");
        assertEquals(result.getAttribute(ATTR_QUERY_ID), original.getAttribute(ATTR_QUERY_ID));
    }

    @Test
    void queryFromPropertyWithExpressionLanguageAndOriginal() throws Exception {
        runner.setIncomingConnection(true);
        runner.setProperty(SQL_QUERY, "SELECT count() AS c FROM ${table}");
        runner.enqueue("ignored", Map.of("table", "types"));
        runner.run();
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        assertEquals(1L, readAvro(runner.getFlowFilesForRelationship(REL_SUCCESS).get(0)).get(0).getAsLong("c"));
    }

    @Test
    void querySettingsReachTheServer() {
        runner.setProperty(SQL_QUERY, "SELECT 1");
        runner.disableControllerService(service);
        runner.setProperty(service, "ch.setting.max_execution_time", "60");
        runner.enableControllerService(service);
        runner.setProperty("ch.setting.max_execution_time", "7");
        runner.setProperty("ch.setting.max_block_size", "4321");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final String queryId = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getAttribute(ATTR_QUERY_ID);
        execute("SYSTEM FLUSH LOGS");
        final GenericRecord log = admin.queryAll("SELECT Settings['max_execution_time'] AS t, Settings['max_block_size'] AS b, read_rows "
                + "FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '" + queryId + "'").get(0);
        assertEquals("7", log.getString("t"));
        assertEquals("4321", log.getString("b"));
    }

    @Test
    void worksWithJsonWriterToo() throws Exception {
        final JsonRecordSetWriter json = new JsonRecordSetWriter();
        runner.addControllerService("json", json);
        runner.setProperty(json, "Output Grouping", "output-oneline");
        runner.enableControllerService(json);
        runner.setProperty(RECORD_WRITER, "json");
        runner.setProperty(SQL_QUERY, "SELECT i8, s, arr, m, t FROM types");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final String content = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent().trim();
        assertEquals("{\"i8\":-5,\"s\":\"Stăvilar\",\"arr\":[1,2,3],\"m\":{\"k\":7},\"t\":{\"_1\":1,\"_2\":\"a\"}}", content);
    }

    private List<Record> readAvro(final MockFlowFile flowFile) throws Exception {
        final AvroReader avro = new AvroReader();
        final TestRunner readerRunner = TestRunners.newTestRunner(QueryClickHouseRecord.class);
        readerRunner.addControllerService("avro", avro);
        readerRunner.enableControllerService(avro);
        final List<Record> records = new java.util.ArrayList<>();
        try (RecordReader reader = avro.createRecordReader(Map.of(), new ByteArrayInputStream(flowFile.toByteArray()), flowFile.getSize(), readerRunner.getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private static void execute(final String sql) {
        try {
            admin.execute(sql).join().close();
        } catch (final Exception e) {
            throw new IllegalStateException(sql, e);
        }
    }
}
