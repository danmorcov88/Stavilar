package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService;
import org.apache.nifi.reporting.InitializationException;
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

import java.util.Map;

import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.ATTR_ERROR;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.ATTR_QUERY_ID;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.CONNECTION_SERVICE;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.REL_FAILURE;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.REL_SUCCESS;
import static io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement.SQL_STATEMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class ExecuteClickHouseStatementIT {

    private static final String IMAGE = System.getProperty("clickhouse.image", "clickhouse/clickhouse-server:26.8");
    private static final String USER = "stavilar";
    private static final String DB = "stavilar";

    @Container
    private static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withEnv("CLICKHOUSE_USER", USER)
            .withEnv("CLICKHOUSE_PASSWORD", USER)
            .withEnv("CLICKHOUSE_DB", DB)
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    private static Client admin;

    private TestRunner runner;

    @BeforeAll
    static void connect() {
        admin = new Client.Builder()
                .addEndpoint("http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123))
                .setUsername(USER).setPassword(USER).setDefaultDatabase(DB)
                .build();
    }

    @AfterAll
    static void cleanUp() {
        admin.close();
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(ExecuteClickHouseStatement.class);
        final StandardClickHouseConnectionService service = new StandardClickHouseConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.setProperty(service, StandardClickHouseConnectionService.ENDPOINTS, CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123));
        runner.setProperty(service, StandardClickHouseConnectionService.USERNAME, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.PASSWORD, USER);
        runner.setProperty(service, StandardClickHouseConnectionService.DATABASE, DB);
        runner.enableControllerService(service);
        runner.setProperty(CONNECTION_SERVICE, "clickhouse");
        runner.setIncomingConnection(false);
    }

    @Test
    void ddlAndDmlLifecycle() {
        run("CREATE TABLE lifecycle (id UInt32, name String) ENGINE = MergeTree ORDER BY id");
        assertEquals("1", scalar("SELECT count() FROM system.tables WHERE database = '" + DB + "' AND name = 'lifecycle'"));

        final MockFlowFile inserted = run("INSERT INTO lifecycle SELECT number, toString(number) FROM numbers(1000)");
        inserted.assertAttributeEquals("clickhouse.summary.written_rows", "1000");
        assertTrue(inserted.getAttribute(ATTR_QUERY_ID).length() > 20);
        assertEquals("1000", scalar("SELECT count() FROM lifecycle"));

        runner.setProperty("ch.setting.mutations_sync", "1");
        run("ALTER TABLE lifecycle DELETE WHERE id < 500");
        assertEquals("500", scalar("SELECT count() FROM lifecycle"));
        runner.removeProperty("ch.setting.mutations_sync");

        run("OPTIMIZE TABLE lifecycle FINAL");
        assertEquals("1", scalar("SELECT count() FROM system.parts WHERE database = '" + DB + "' AND table = 'lifecycle' AND active"));

        run("TRUNCATE TABLE lifecycle");
        assertEquals("0", scalar("SELECT count() FROM lifecycle"));

        run("DROP TABLE lifecycle");
        assertEquals("0", scalar("SELECT count() FROM system.tables WHERE database = '" + DB + "' AND name = 'lifecycle'"));
    }

    @Test
    void statementFromFlowFileContentKeepsAttributes() {
        runner.setIncomingConnection(true);
        runner.enqueue("CREATE TABLE from_content (id UInt32) ENGINE = Memory", Map.of("marker", "x"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        out.assertAttributeEquals("marker", "x");
        out.assertContentEquals("CREATE TABLE from_content (id UInt32) ENGINE = Memory");
        assertEquals("1", scalar("SELECT count() FROM system.tables WHERE database = '" + DB + "' AND name = 'from_content'"));
    }

    @Test
    void expressionLanguageInStatement() {
        runner.setIncomingConnection(true);
        runner.setProperty(SQL_STATEMENT, "CREATE TABLE ${table} (id UInt32) ENGINE = Memory");
        runner.enqueue("ignored", Map.of("table", "from_el"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertEquals("1", scalar("SELECT count() FROM system.tables WHERE database = '" + DB + "' AND name = 'from_el'"));
    }

    @Test
    void serverErrorGoesToFailureWithMessage() {
        runner.setProperty(SQL_STATEMENT, "DROP TABLE does_not_exist");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        assertTrue(out.getAttribute(ATTR_ERROR).contains("does_not_exist"), out.getAttribute(ATTR_ERROR));
        assertTrue(out.getAttribute(ATTR_QUERY_ID).length() > 20);
    }

    @Test
    void selectIsAcceptedAndSummarised() {
        final MockFlowFile out = run("SELECT number FROM numbers(42)");
        out.assertAttributeEquals("clickhouse.summary.result_rows", "42");
    }

    @Test
    void settingsReachTheServer() {
        runner.setProperty("ch.setting.max_execution_time", "9");
        final MockFlowFile out = run("SELECT 1");
        execute("SYSTEM FLUSH LOGS");
        assertEquals("9", scalar("SELECT Settings['max_execution_time'] FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '"
                + out.getAttribute(ATTR_QUERY_ID) + "'"));
    }

    private MockFlowFile run(final String sql) {
        runner.clearTransferState();
        runner.setProperty(SQL_STATEMENT, sql);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
    }

    private static String scalar(final String sql) {
        return admin.queryAll(sql).get(0).getString(1);
    }

    private static void execute(final String sql) {
        try {
            admin.execute(sql).join().close();
        } catch (final Exception e) {
            throw new IllegalStateException(sql, e);
        }
    }
}
