package io.github.danmorcov88.stavilar.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.ATTR_QUERY_ID;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.MAX_ROWS_PER_FLOWFILE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.RECORD_WRITER;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_ORIGINAL;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord.SQL_QUERY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryClickHouseRecordTest {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(QueryClickHouseRecord.class);
        final StubConnectionService service = new StubConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.enableControllerService(service);
        final MockRecordWriter writer = new MockRecordWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
        runner.setProperty(RECORD_WRITER, "writer");
    }

    @Test
    void minimalConfigIsValid() {
        runner.assertValid();
        runner.setProperty(SQL_QUERY, "SELECT 1");
        runner.setProperty(MAX_ROWS_PER_FLOWFILE, "1000");
        runner.setProperty("ch.setting.max_execution_time", "60");
        runner.assertValid();
    }

    @Test
    void rejectsBadValues() {
        runner.setProperty(MAX_ROWS_PER_FLOWFILE, "-1");
        runner.assertNotValid();
        runner.setProperty(MAX_ROWS_PER_FLOWFILE, "0");
        runner.setProperty("max_execution_time", "60");
        runner.assertNotValid();
    }

    @Test
    void noQueryAndNoInputDoesNothing() {
        runner.setIncomingConnection(false);
        runner.run();
        runner.assertTransferCount(REL_SUCCESS, 0);
        runner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    void emptyIncomingQueryGoesToFailure() {
        runner.enqueue("   ");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    void connectionRefusedWithoutInputCreatesFailureFlowFile() {
        runner.setIncomingConnection(false);
        runner.setProperty(SQL_QUERY, "SELECT 1");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        assertTrue(out.getAttribute(ATTR_ERROR).contains("Connection refused"), out.getAttribute(ATTR_ERROR));
        assertFalse(out.getAttribute(ATTR_QUERY_ID).isBlank());
    }

    @Test
    void connectionRefusedWithInputRoutesInputToFailure() {
        runner.enqueue("SELECT 1", java.util.Map.of("marker", "x"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.assertTransferCount(REL_ORIGINAL, 0);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        out.assertAttributeEquals("marker", "x");
        out.assertContentEquals("SELECT 1");
        assertTrue(out.getAttribute(ATTR_ERROR).contains("Connection refused"), out.getAttribute(ATTR_ERROR));
    }
}
