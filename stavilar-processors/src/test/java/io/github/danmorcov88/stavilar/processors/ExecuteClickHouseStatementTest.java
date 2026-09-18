package io.github.danmorcov88.stavilar.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.ATTR_QUERY_ID;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.REL_RETRY;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.SQL_STATEMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteClickHouseStatementTest {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(ExecuteClickHouseStatement.class);
        final StubConnectionService service = new StubConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.enableControllerService(service);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
    }

    @Test
    void validation() {
        runner.assertValid();
        runner.setProperty(SQL_STATEMENT, "OPTIMIZE TABLE t FINAL");
        runner.setProperty("ch.setting.mutations_sync", "1");
        runner.assertValid();
        runner.setProperty("mutations_sync", "1");
        runner.assertNotValid();
    }

    @Test
    void noStatementAndNoInputDoesNothing() {
        runner.setIncomingConnection(false);
        runner.run();
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);
    }

    @Test
    void blankIncomingStatementGoesToFailure() {
        runner.enqueue("  ");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    void connectionRefusedGoesToRetry() {
        runner.enqueue("TRUNCATE TABLE t", Map.of("marker", "x"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_RETRY).get(0);
        out.assertAttributeEquals("marker", "x");
        assertTrue(out.getAttribute(ATTR_ERROR).contains("Connection refused"), out.getAttribute(ATTR_ERROR));
        assertFalse(out.getAttribute(ATTR_QUERY_ID).isBlank());
    }

    @Test
    void connectionRefusedWithoutInputCreatesFlowFile() {
        runner.setIncomingConnection(false);
        runner.setProperty(SQL_STATEMENT, "TRUNCATE TABLE t");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);
    }

    @Test
    void parsesSummaryHeader() {
        final Map<String, String> attributes = ClickHouseSummary.parse(
                "{\"read_rows\":\"2\",\"read_bytes\":\"18\",\"written_rows\":\"2\",\"elapsed_ns\":\"60800784\",\"nested\":{\"x\":1},\"memory_usage\":\"4197420\"}");
        assertEquals("2", attributes.get("clickhouse.summary.read_rows"));
        assertEquals("60800784", attributes.get("clickhouse.summary.elapsed_ns"));
        assertEquals("4197420", attributes.get("clickhouse.summary.memory_usage"));
        assertFalse(attributes.containsKey("clickhouse.summary.nested"));
        assertEquals(Map.of(), ClickHouseSummary.parse("not json"));
        assertEquals(Map.of(), ClickHouseSummary.parse(""));
        assertEquals(Map.of(), ClickHouseSummary.toAttributes(Map.of("Content-Type", "text/plain")));
        assertEquals("7", ClickHouseSummary.toAttributes(Map.of("x-clickhouse-summary", "{\"result_rows\":\"7\"}")).get("clickhouse.summary.result_rows"));
    }
}
