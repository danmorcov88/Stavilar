package io.github.danmorcov88.stavilar.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.DATABASE;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.ENDPOINTS;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.PASSWORD;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class StandardClickHouseConnectionServiceIT {

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

    private TestRunner runner;
    private StandardClickHouseConnectionService service;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardClickHouseConnectionService();
        runner.addControllerService("clickhouse", service);
        runner.setProperty(service, ENDPOINTS, CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123));
        runner.setProperty(service, USERNAME, USER);
        runner.setProperty(service, PASSWORD, USER);
        runner.setProperty(service, DATABASE, DB);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void enablesAndQueries() {
        runner.enableControllerService(service);
        final Client client = service.getClient();

        assertTrue(client.ping());
        assertEquals(1, client.queryAll("SELECT 1 AS v").get(0).getInteger("v"));
        assertEquals(DB, client.queryAll("SELECT currentDatabase() AS v").get(0).getString("v"));
        assertEquals(DB, service.getDatabase());
        assertTrue(service.getDefaultSettings().isEmpty());
    }

    @Test
    void serverSettingsReachTheServer() {
        runner.setProperty(service, "ch.setting.max_threads", "3");
        runner.setProperty(service, "ch.setting.max_block_size", "4096");
        runner.enableControllerService(service);

        final List<GenericRecord> rows = service.getClient()
                .queryAll("SELECT getSetting('max_threads') AS t, getSetting('max_block_size') AS b");
        assertEquals("3", rows.get(0).getString("t"));
        assertEquals("4096", rows.get(0).getString("b"));
        assertEquals(List.of("max_threads", "max_block_size"), List.copyOf(service.getDefaultSettings().keySet()));
    }

    @Test
    void wrongPasswordKeepsServiceDisabled() {
        runner.setProperty(service, PASSWORD, "wrong");
        assertThrows(Throwable.class, () -> runner.enableControllerService(service));
        assertThrows(IllegalStateException.class, service::getClient);
    }

    @Test
    void unreachableEndpointKeepsServiceDisabled() {
        runner.setProperty(service, ENDPOINTS, "localhost:1");
        runner.setProperty(service, StandardClickHouseConnectionService.CONNECT_TIMEOUT, "2 secs");
        assertThrows(Throwable.class, () -> runner.enableControllerService(service));
        assertThrows(IllegalStateException.class, service::getClient);
    }

    @Test
    void disableClosesClientAndLeavesNoThreads() throws InterruptedException {
        final Set<String> before = threadNames();
        runner.enableControllerService(service);
        service.getClient().queryAll("SELECT 1");

        runner.disableControllerService(service);
        assertThrows(IllegalStateException.class, service::getClient);

        final long deadline = System.currentTimeMillis() + 5_000;
        Set<String> leftover = leftoverThreads(before);
        while (!leftover.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            leftover = leftoverThreads(before);
        }
        assertEquals(Set.of(), leftover);
    }

    private static Set<String> threadNames() {
        return Thread.getAllStackTraces().keySet().stream().map(Thread::getName).collect(Collectors.toSet());
    }

    /** Threads started since {@code before}, ignoring the ones Testcontainers starts on its own. */
    private static Set<String> leftoverThreads(final Set<String> before) {
        return threadNames().stream()
                .filter(name -> !before.contains(name))
                .filter(name -> !name.contains("testcontainers") && !name.contains("ducttape") && !name.contains("docker"))
                .collect(Collectors.toSet());
    }
}
