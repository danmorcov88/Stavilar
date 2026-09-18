package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.TransportException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ASYNC_INSERT;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_INSERTS;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.ATTR_ROWS_WRITTEN;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.DATABASE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.DEDUPLICATION_TOKEN;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.DEDUPLICATION_TOKEN_ATTRIBUTE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.FORMAT_JSON_EACH_ROW;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.FORMAT_ROW_BINARY;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.INSERT_FORMAT;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.RECORD_READER;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.REL_RETRY;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TABLE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TOKEN_ATTRIBUTE;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.TOKEN_FLOWFILE_UUID;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.WAIT_END_OF_QUERY;
import static io.github.danmorcov88.stavilar.processors.PutClickHouseRecord.batchToken;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PutClickHouseRecordTest {

    private TestRunner runner;
    private MockRecordParser reader;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(PutClickHouseRecord.class);
        final StubConnectionService service = new StubConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.enableControllerService(service);
        reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("name", RecordFieldType.STRING);
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
        runner.setProperty(RECORD_READER, "reader");
        runner.setProperty(TABLE, "events");
    }

    @Test
    void minimalConfigIsValid() {
        runner.assertValid();
    }

    @Test
    void tokenAndAsyncCannotBeCombined() {
        runner.setProperty(DEDUPLICATION_TOKEN, TOKEN_FLOWFILE_UUID);
        runner.setProperty(ASYNC_INSERT, "true");
        runner.assertNotValid();
    }

    @Test
    void attributeTokenNeedsAttributeName() {
        runner.setProperty(DEDUPLICATION_TOKEN, TOKEN_ATTRIBUTE);
        runner.assertNotValid();
        runner.setProperty(DEDUPLICATION_TOKEN_ATTRIBUTE, "batch.id");
        runner.assertValid();
    }

    @Test
    void rejectsDynamicPropertyWithoutPrefix() {
        runner.setProperty("max_threads", "1");
        runner.assertNotValid();
        runner.removeProperty("max_threads");
        runner.setProperty("ch.setting.max_threads", "1");
        runner.assertValid();
    }

    @Test
    void batchTokens() {
        assertNull(batchToken(null, 0));
        assertNull(batchToken(null, 3));
        assertEquals("abc", batchToken("abc", 0));
        assertEquals("abc-1", batchToken("abc", 1));
        assertEquals("abc-12", batchToken("abc", 12));
    }

    @Test
    void targetTableUsesDatabaseWhenSet() {
        final MockFlowFile flowFile = new MockFlowFile(1);
        flowFile.putAttributes(Map.of("tbl", "from_attr"));
        assertEquals("events", PutClickHouseRecord.targetTable(runner.getProcessContext(), flowFile));

        runner.setProperty(DATABASE, "analytics");
        runner.setProperty(TABLE, "${tbl}");
        assertEquals("analytics.from_attr", PutClickHouseRecord.targetTable(runner.getProcessContext(), flowFile));
    }

    @Test
    void insertSettingsDefaultsRowBinary() {
        final Map<String, String> settings = PutClickHouseRecord.insertSettings(runner.getProcessContext(), new MockFlowFile(1));
        assertEquals(Map.of("async_insert", "0", "wait_end_of_query", "1"), settings);
    }

    @Test
    void insertSettingsDefaultsJson() {
        runner.setProperty(INSERT_FORMAT, FORMAT_JSON_EACH_ROW);
        final Map<String, String> settings = PutClickHouseRecord.insertSettings(runner.getProcessContext(), new MockFlowFile(1));
        assertEquals("best_effort", settings.get("date_time_input_format"));
        assertEquals("0", settings.get("input_format_skip_unknown_fields"));
        assertEquals("0", settings.get("async_insert"));
        assertEquals("1", settings.get("wait_end_of_query"));
        assertFalse(settings.containsKey("wait_for_async_insert"));
    }

    @Test
    void insertFormatDefaultsToRowBinary() {
        assertEquals(FORMAT_ROW_BINARY, runner.getProcessContext().getProperty(INSERT_FORMAT).getValue());
        runner.setProperty(INSERT_FORMAT, "CSV");
        runner.assertNotValid();
    }

    @Test
    void insertSettingsAsyncAndOverrides() {
        runner.setProperty(INSERT_FORMAT, FORMAT_JSON_EACH_ROW);
        runner.setProperty(ASYNC_INSERT, "true");
        runner.setProperty(WAIT_END_OF_QUERY, "false");
        runner.setProperty("ch.setting.date_time_input_format", "basic");
        runner.setProperty("ch.setting.max_threads", "${threads}");
        final MockFlowFile flowFile = new MockFlowFile(1);
        flowFile.putAttributes(Map.of("threads", "4"));

        final Map<String, String> settings = PutClickHouseRecord.insertSettings(runner.getProcessContext(), flowFile);
        assertEquals("1", settings.get("async_insert"));
        assertEquals("1", settings.get("wait_for_async_insert"));
        assertFalse(settings.containsKey("wait_end_of_query"));
        assertEquals("basic", settings.get("date_time_input_format"));
        assertEquals("4", settings.get("max_threads"));
    }

    @Test
    void emptyFlowFileSucceedsWithoutInsert() {
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals(ATTR_ROWS_WRITTEN, "0");
        out.assertAttributeEquals(ATTR_INSERTS, "0");
    }

    @ParameterizedTest
    @ValueSource(strings = {FORMAT_ROW_BINARY, FORMAT_JSON_EACH_ROW})
    void connectionRefusedGoesToRetry(final String format) {
        runner.setProperty(INSERT_FORMAT, format);
        reader.addRecord(1, "a");
        runner.enqueue("x");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_RETRY).get(0);
        assertTrue(out.getAttribute(ATTR_ERROR).contains("Connection refused"), out.getAttribute(ATTR_ERROR));
    }

    @Test
    void unreadableRecordsGoToFailure() {
        reader.addRecord(1, "a");
        reader.failAfter(0);
        runner.enqueue("x");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        assertFalse(runner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ATTR_ERROR).isBlank());
    }

    @Test
    void missingTokenAttributeGoesToFailure() {
        runner.setProperty(DEDUPLICATION_TOKEN, TOKEN_ATTRIBUTE);
        runner.setProperty(DEDUPLICATION_TOKEN_ATTRIBUTE, "batch.id");
        reader.addRecord(1, "a");
        runner.enqueue("x");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        assertTrue(runner.getFlowFilesForRelationship(REL_FAILURE).get(0).getAttribute(ATTR_ERROR).contains("batch.id"));
    }

    @Test
    void errorClassification() {
        assertTrue(InsertErrors.isRetryable(new ConnectionInitiationException("refused")));
        assertTrue(InsertErrors.isRetryable(new CompletionException(new ConnectionInitiationException("refused"))));
        assertTrue(InsertErrors.isRetryable(new TransportException("reset", null, "q")));
        assertTrue(InsertErrors.isRetryable(new DataTransferException("broken pipe")));
        assertFalse(InsertErrors.isRetryable(new ClientMisconfigurationException("bad option")));
        assertFalse(InsertErrors.isRetryable(new IllegalStateException("other")));
        // client-v2 marks 252 TOO_MANY_PARTS and 241 MEMORY_LIMIT_EXCEEDED retryable; 60 UNKNOWN_TABLE and 117 INCORRECT_DATA are not
        assertTrue(InsertErrors.isRetryable(new ServerException(252, "Too many parts", 500, "")));
        assertTrue(InsertErrors.isRetryable(new ServerException(241, "Memory limit", 500, "")));
        assertFalse(InsertErrors.isRetryable(new ServerException(60, "Table not found", 404, "")));
        assertFalse(InsertErrors.isRetryable(new ServerException(117, "Unknown field", 400, "")));
        assertEquals("Table not found", InsertErrors.message(new CompletionException(new ServerException(60, "Table not found", 404, ""))));
    }

    @Test
    void reportsAllRelationships() {
        assertEquals(List.of("failure", "retry", "success"),
                runner.getProcessor().getRelationships().stream().map(r -> r.getName()).sorted().toList());
    }
}
