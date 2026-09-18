package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ASYNC_INSERT;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_INSERTS;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_QUERY_ID;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_ROWS_WRITTEN;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.DEDUPLICATION_TOKEN;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.DEDUPLICATION_TOKEN_ATTRIBUTE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.FORMAT_JSON_EACH_ROW;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.FORMAT_ROW_BINARY;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.INSERT_FORMAT;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.MAX_ROWS_PER_INSERT;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.RECORD_READER;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TABLE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TOKEN_ATTRIBUTE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TOKEN_FLOWFILE_UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PutClickHouseRecordIT {

    private static final String IMAGE = System.getProperty("clickhouse.image", "clickhouse/clickhouse-server:26.8");
    private static final String USER = "stavilar";
    private static final String DB = "stavilar";
    private static final int ONE_MILLION = 1_000_000;

    @Container
    private static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withEnv("CLICKHOUSE_USER", USER)
            .withEnv("CLICKHOUSE_PASSWORD", USER)
            .withEnv("CLICKHOUSE_DB", DB)
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    private static Client admin;
    private static Path bigCsv;

    private TestRunner runner;
    private StandardClickHouseConnectionService service;

    @BeforeAll
    static void createTablesAndData() throws IOException {
        admin = new Client.Builder()
                .addEndpoint("http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123))
                .setUsername(USER).setPassword(USER).setDefaultDatabase(DB)
                .build();
        execute("CREATE TABLE big (id UInt64, name String, ts DateTime64(3, 'UTC'), amount Decimal(18, 4)) "
                + "ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE dedup (id UInt32, name String) ENGINE = MergeTree ORDER BY id "
                + "SETTINGS non_replicated_deduplication_window = 100");
        execute("CREATE TABLE plain (id UInt32, name String) ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE defaults (id UInt32, name String DEFAULT 'none', created DateTime DEFAULT now(), score Nullable(Float64)) "
                + "ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE evolving (id UInt32) ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE with_map (id UInt32, tags Map(String, String)) ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE types ("
                + "i8 Int8, i64 Int64, u64 UInt64, f32 Float32, f64 Float64, dec Decimal(12, 3), "
                + "s String, fs FixedString(3), b Bool, u UUID, "
                + "d Date, d32 Date32, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), "
                + "e Enum8('a' = 1, 'b' = 2), n Nullable(String), lc LowCardinality(String), arr Array(Int32)"
                + ") ENGINE = MergeTree ORDER BY i64");

        bigCsv = Files.createTempFile("stavilar-big", ".csv");
        try (BufferedWriter w = Files.newBufferedWriter(bigCsv, StandardCharsets.UTF_8)) {
            w.write("id,name,ts,amount\n");
            for (int i = 1; i <= ONE_MILLION; i++) {
                w.write(i + ",name-" + i + ",2024-01-01T00:00:00." + String.format("%03d", i % 1000) + "Z," + i + ".2500\n");
            }
        }
    }

    @AfterAll
    static void cleanUp() throws IOException {
        admin.close();
        Files.deleteIfExists(bigCsv);
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(PutClickHouseRecord.class);
        service = new StandardClickHouseConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.setProperty(service, StandardClickHouseConnectionService.ENDPOINTS, CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123));
        runner.setProperty(service, StandardClickHouseConnectionService.USERNAME, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.PASSWORD, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.DATABASE, DB);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
    }

    @ParameterizedTest
    @ValueSource(strings = {FORMAT_ROW_BINARY, FORMAT_JSON_EACH_ROW})
    void insertsOneMillionRowsInBatches(final String format) throws Exception {
        execute("TRUNCATE TABLE big");
        useCsvReader();
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "big");
        runner.setProperty(INSERT_FORMAT, format);
        runner.enqueue(bigCsv);

        final long start = System.nanoTime();
        runner.run();
        final long millis = (System.nanoTime() - start) / 1_000_000;

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals(ATTR_ROWS_WRITTEN, String.valueOf(ONE_MILLION));
        out.assertAttributeEquals(ATTR_INSERTS, "10");
        assertEquals(10, out.getAttribute(ATTR_QUERY_ID).split(",").length);

        assertEquals(ONE_MILLION, count("big"));
        assertEquals("500000500000", scalar("SELECT sum(id) FROM big"));
        assertEquals("2024-01-01 00:00:00.007", scalar("SELECT toString(ts) FROM big WHERE id = 7"));
        assertEquals("7.25", scalar("SELECT toString(amount) FROM big WHERE id = 7"));
        assertEquals("10", scalar("SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ("
                + quotedList(out.getAttribute(ATTR_QUERY_ID)) + ")"));
        System.out.println("1M rows inserted as " + format + " in " + millis + " ms");
    }

    @Test
    void deduplicationTokenPreventsDuplicatesOnRetry() throws Exception {
        final MockRecordParser reader = useMockReader();
        reader.addRecord(1, "a");
        reader.addRecord(2, "b");
        reader.addRecord(3, "c");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "dedup");
        runner.setProperty(DEDUPLICATION_TOKEN, TOKEN_FLOWFILE_UUID);

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile first = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        first.assertAttributeEquals(ATTR_ROWS_WRITTEN, "3");
        assertEquals(3, count("dedup"));

        // same FlowFile again, as a retry loop would do: same uuid, same token (the mock reader replays its records)
        runner.clearTransferState();
        runner.enqueue(first);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile second = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals(3, count("dedup"));

        final String uuid = first.getAttribute("uuid");
        assertEquals(uuid, scalar("SELECT Settings['insert_deduplication_token'] FROM system.query_log "
                + "WHERE type = 'QueryFinish' AND query_id = '" + second.getAttribute(ATTR_QUERY_ID) + "'"));
    }

    @Test
    void multipleInsertsGetIndexedTokens() throws Exception {
        final MockRecordParser reader = useMockReader();
        for (int i = 1; i <= 5; i++) {
            reader.addRecord(i, "n" + i);
        }
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "dedup");
        runner.setProperty(MAX_ROWS_PER_INSERT, "2");
        runner.setProperty(DEDUPLICATION_TOKEN, TOKEN_ATTRIBUTE);
        runner.setProperty(DEDUPLICATION_TOKEN_ATTRIBUTE, "batch.id");

        runner.enqueue("x", Map.of("batch.id", "load-42"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals(ATTR_INSERTS, "3");
        out.assertAttributeEquals(ATTR_ROWS_WRITTEN, "5");

        final List<GenericRecord> tokens = queryLog("SELECT Settings['insert_deduplication_token'] AS t FROM system.query_log "
                + "WHERE type = 'QueryFinish' AND query_id IN (" + quotedList(out.getAttribute(ATTR_QUERY_ID)) + ") ORDER BY event_time_microseconds");
        assertEquals(List.of("load-42", "load-42-1", "load-42-2"), tokens.stream().map(r -> r.getString("t")).toList());

        // the same load again does not add rows
        runner.clearTransferState();
        runner.enqueue("x", Map.of("batch.id", "load-42"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertEquals("5", scalar("SELECT count() FROM dedup WHERE name LIKE 'n%'"));
    }

    @ParameterizedTest
    @ValueSource(strings = {FORMAT_ROW_BINARY, FORMAT_JSON_EACH_ROW})
    void unknownColumnGoesToFailureWithServerMessage(final String format) throws Exception {
        runner.setProperty(INSERT_FORMAT, format);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("nope", RecordFieldType.STRING);
        reader.addRecord(1, "x");
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "plain");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final String error = runner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ATTR_ERROR);
        assertTrue(error.contains("nope"), error);
        assertEquals(0, count("plain"));
    }

    @ParameterizedTest
    @ValueSource(strings = {FORMAT_ROW_BINARY, FORMAT_JSON_EACH_ROW})
    void unknownTableGoesToFailure(final String format) throws Exception {
        runner.setProperty(INSERT_FORMAT, format);
        useMockReader().addRecord(1, "x");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "does_not_exist");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        assertTrue(runner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ATTR_ERROR).contains("does_not_exist"));
    }

    @Test
    void asyncInsertWritesRows() throws Exception {
        useMockReader().addRecord(100, "async");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "plain");
        runner.setProperty(ASYNC_INSERT, "true");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("1", scalar("SELECT count() FROM plain WHERE id = 100"));
        // query_log.Settings lists only settings that differ from the user's defaults, and 26.x defaults to async_insert=1;
        // the asynchronous insert log is the proof that the insert went through the async path
        assertEquals("1", scalar("SELECT count() FROM system.asynchronous_insert_log WHERE query_id = '"
                + out.getAttribute(ATTR_QUERY_ID) + "' AND status = 'Ok'"));
    }

    @Test
    void processorSettingOverridesServiceSetting() throws Exception {
        useMockReader().addRecord(200, "settings");
        runner.setProperty(service, "ch.setting.max_threads", "2");
        runner.setProperty(service, "ch.setting.max_block_size", "1234");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "plain");
        runner.setProperty("ch.setting.max_threads", "1");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final String queryId = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getAttribute(ATTR_QUERY_ID);
        // wait_end_of_query is an HTTP parameter and format settings are not logged, so only query settings are checked here
        final GenericRecord row = queryLog("SELECT Settings['max_threads'] AS t, Settings['max_block_size'] AS b, Settings['async_insert'] AS a "
                + "FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '" + queryId + "'").get(0);
        assertEquals("1", row.getString("t"), "processor setting overrides the service setting");
        assertEquals("1234", row.getString("b"), "service setting still applies");
        assertEquals("0", row.getString("a"), "async_insert=0 is sent explicitly");
    }

    @ParameterizedTest
    @ValueSource(strings = {FORMAT_ROW_BINARY, FORMAT_JSON_EACH_ROW})
    void allTypesRoundTrip(final String format) throws Exception {
        execute("TRUNCATE TABLE types");
        runner.setProperty(INSERT_FORMAT, format);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("i8", RecordFieldType.BYTE);
        reader.addSchemaField("i64", RecordFieldType.LONG);
        reader.addSchemaField("u64", RecordFieldType.BIGINT);
        reader.addSchemaField("f32", RecordFieldType.FLOAT);
        reader.addSchemaField("f64", RecordFieldType.DOUBLE);
        reader.addSchemaField("dec", RecordFieldType.DECIMAL);
        reader.addSchemaField("s", RecordFieldType.STRING);
        reader.addSchemaField("fs", RecordFieldType.STRING);
        reader.addSchemaField("b", RecordFieldType.BOOLEAN);
        reader.addSchemaField("u", RecordFieldType.UUID);
        reader.addSchemaField("d", RecordFieldType.DATE);
        reader.addSchemaField("d32", RecordFieldType.DATE);
        reader.addSchemaField("dt", RecordFieldType.TIMESTAMP);
        reader.addSchemaField("dt64", RecordFieldType.TIMESTAMP);
        reader.addSchemaField("e", RecordFieldType.STRING);
        reader.addSchemaField("n", RecordFieldType.STRING);
        reader.addSchemaField("lc", RecordFieldType.STRING);
        reader.addSchemaField("arr", RecordFieldType.ARRAY);
        final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        final Instant instant = Instant.parse("2024-03-15T11:45:10.123Z");
        reader.addRecord((byte) -5, 9_000_000_000L, new java.math.BigInteger("18446744073709551615"), 1.5f, 2.25,
                new BigDecimal("12345.678"), "Stăvilar \"quoted\"", "abc", true, uuid,
                java.sql.Date.valueOf("2024-03-15"), java.time.LocalDate.of(1950, 1, 2),
                Timestamp.from(instant), instant.atOffset(java.time.ZoneOffset.ofHours(3)),
                "b", null, "low", new Object[]{1, 2, 3});
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "types");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final GenericRecord row = admin.queryAll("SELECT toString(i8) i8, toString(i64) i64, toString(u64) u64, toString(f32) f32, "
                + "toString(f64) f64, toString(dec) dec, s, toString(fs) fs, toString(b) b, toString(u) u, toString(d) d, toString(d32) d32, "
                + "toString(dt) dt, toString(dt64) dt64, toString(e) e, toString(n) n, toString(lc) lc, toString(arr) arr FROM types").get(0);
        assertEquals("-5", row.getString("i8"));
        assertEquals("9000000000", row.getString("i64"));
        assertEquals("18446744073709551615", row.getString("u64"));
        assertEquals("1.5", row.getString("f32"));
        assertEquals("2.25", row.getString("f64"));
        assertEquals("12345.678", row.getString("dec"));
        assertEquals("Stăvilar \"quoted\"", row.getString("s"));
        assertEquals("abc", row.getString("fs"));
        assertEquals("true", row.getString("b"));
        assertEquals(uuid.toString(), row.getString("u"));
        assertEquals("2024-03-15", row.getString("d"));
        assertEquals("1950-01-02", row.getString("d32"));
        assertEquals("2024-03-15 11:45:10", row.getString("dt"));
        assertEquals("2024-03-15 11:45:10.123", row.getString("dt64"));
        assertEquals("b", row.getString("e"));
        assertNull(row.getString("n"));
        assertEquals("low", row.getString("lc"));
        assertEquals("[1,2,3]", row.getString("arr"));
    }

    @Test
    void rowBinaryLeavesMissingColumnsToTheirDefaults() throws Exception {
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("score", RecordFieldType.DOUBLE);
        reader.addRecord(1, 2.5);
        reader.addRecord(2, null);
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "defaults");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final List<GenericRecord> rows = admin.queryAll("SELECT id, name, created > now() - 60 AS recent, toString(score) AS score FROM defaults ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("none", rows.get(0).getString("name"));
        assertEquals("1", rows.get(0).getString("recent"));
        assertEquals("2.5", rows.get(0).getString("score"));
        assertNull(rows.get(1).getString("score"));
    }

    @Test
    void rowBinaryReadsSchemaAgainAfterAlterTable() throws Exception {
        useMockReader().addRecord(1, "ignored");
        // first FlowFile: only 'id' exists; cache the schema with a reader that has just 'id'
        final MockRecordParser idOnly = new MockRecordParser();
        idOnly.addSchemaField("id", RecordFieldType.INT);
        idOnly.addRecord(1);
        runner.addControllerService("idOnly", idOnly);
        runner.enableControllerService(idOnly);
        runner.setProperty(RECORD_READER, "idOnly");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "evolving");
        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.clearTransferState();

        execute("ALTER TABLE evolving ADD COLUMN name String");

        // second FlowFile has the new column; the stale cache must be refreshed without a restart
        runner.setProperty(RECORD_READER, "reader");
        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertEquals("ignored", scalar("SELECT name FROM evolving WHERE name != '' LIMIT 1"));
    }

    @Test
    void rowBinaryRejectsUnsupportedColumnTypeWithHint() throws Exception {
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("tags", RecordFieldType.MAP);
        reader.addRecord(1, Map.of("k", "v"));
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
        runner.enableControllerService(service);
        runner.setProperty(TABLE, "with_map");

        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final String error = runner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ATTR_ERROR);
        assertTrue(error.contains("Map(String, String)") && error.contains("JSONEachRow"), error);

        // the same records go through with JSONEachRow
        runner.clearTransferState();
        runner.setProperty(INSERT_FORMAT, FORMAT_JSON_EACH_ROW);
        runner.enqueue("x");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertEquals("v", scalar("SELECT tags['k'] FROM with_map"));
    }

    private void useCsvReader() throws InitializationException {
        final CSVReader reader = new CSVReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, "Schema Access Strategy", "csv-header-derived");
        runner.setProperty(reader, "Treat First Line as Header", "true");
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
    }

    private MockRecordParser useMockReader() throws InitializationException {
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("name", RecordFieldType.STRING);
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(RECORD_READER, "reader");
        return reader;
    }

    private static long count(final String table) {
        return Long.parseLong(scalar("SELECT count() FROM " + table));
    }

    /** Runs a one-value query; flushes the logs first so system.query_log is current. */
    private static String scalar(final String sql) {
        final List<GenericRecord> rows = queryLog(sql);
        return rows.isEmpty() ? null : rows.get(0).getString(1);
    }

    private static List<GenericRecord> queryLog(final String sql) {
        execute("SYSTEM FLUSH LOGS");
        return admin.queryAll(sql);
    }

    private static void execute(final String sql) {
        try {
            admin.execute(sql).join().close();
        } catch (final Exception e) {
            throw new IllegalStateException(sql, e);
        }
    }

    private static String quotedList(final String csv) {
        return "'" + csv.replace(",", "','") + "'";
    }
}
