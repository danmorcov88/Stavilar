package io.github.danmorcov88.stavilar.service;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.CONNECT_TIMEOUT;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.ENDPOINTS;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.SSL_CONTEXT_SERVICE;
import static io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService.USE_TLS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardClickHouseConnectionServiceTest {

    private TestRunner runner;
    private StandardClickHouseConnectionService service;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardClickHouseConnectionService();
        runner.addControllerService("clickhouse", service);
    }

    @Test
    void minimalConfigIsValid() {
        runner.setProperty(service, ENDPOINTS, "localhost:8123");
        runner.assertValid(service);
    }

    @Test
    void endpointsAreRequired() {
        runner.assertNotValid(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "localhost:abc", "localhost:70000", "localhost:0", ":8123", "localhost:", " , "})
    void rejectsMalformedEndpoints(final String value) {
        runner.setProperty(service, ENDPOINTS, value);
        runner.assertNotValid(service);
    }

    @Test
    void acceptsMultipleEndpoints() {
        runner.setProperty(service, ENDPOINTS, "ch1:8123, ch2:8443,ch3:9000");
        runner.assertValid(service);
        assertEquals(3, EndpointsValidator.parse("ch1:8123, ch2:8443,ch3:9000").size());
    }

    @Test
    void parsesHostAndPort() {
        final var endpoints = EndpointsValidator.parse("ch1:8123, ch2:8443");
        assertEquals("ch1", endpoints.get(0).host());
        assertEquals(8123, endpoints.get(0).port());
        assertEquals("ch2", endpoints.get(1).host());
        assertEquals(8443, endpoints.get(1).port());
    }

    @Test
    void rejectsEmptyEndpointList() {
        assertThrows(IllegalArgumentException.class, () -> EndpointsValidator.parse(""));
        assertThrows(IllegalArgumentException.class, () -> EndpointsValidator.parse(null));
    }

    @Test
    void acceptsSettingDynamicProperty() {
        runner.setProperty(service, ENDPOINTS, "localhost:8123");
        runner.setProperty(service, "ch.setting.max_threads", "2");
        runner.assertValid(service);
    }

    @Test
    void rejectsDynamicPropertyWithoutPrefix() {
        runner.setProperty(service, ENDPOINTS, "localhost:8123");
        runner.setProperty(service, "max_threads", "2");
        runner.assertNotValid(service);
    }

    @Test
    void rejectsEmptySettingValue() {
        runner.setProperty(service, ENDPOINTS, "localhost:8123");
        runner.setProperty(service, "ch.setting.max_threads", "");
        runner.assertNotValid(service);
    }

    @Test
    void rejectsBadTimePeriod() {
        runner.setProperty(service, ENDPOINTS, "localhost:8123");
        runner.setProperty(service, CONNECT_TIMEOUT, "ten seconds");
        runner.assertNotValid(service);
    }

    @Test
    void sslServiceNeedsTrustStore() throws InitializationException {
        final StubSslContextService ssl = new StubSslContextService(null);
        runner.addControllerService("ssl", ssl);
        runner.enableControllerService(ssl);
        runner.setProperty(service, ENDPOINTS, "localhost:8443");
        runner.setProperty(service, USE_TLS, "true");
        runner.setProperty(service, SSL_CONTEXT_SERVICE, "ssl");
        runner.assertNotValid(service);
    }

    @Test
    void sslServiceWithTrustStoreIsValid() throws InitializationException {
        final StubSslContextService ssl = new StubSslContextService("/etc/nifi/truststore.p12");
        runner.addControllerService("ssl", ssl);
        runner.enableControllerService(ssl);
        runner.setProperty(service, ENDPOINTS, "localhost:8443");
        runner.setProperty(service, USE_TLS, "true");
        runner.setProperty(service, SSL_CONTEXT_SERVICE, "ssl");
        runner.assertValid(service);
    }

    @Test
    void getClientBeforeEnableFails() {
        assertThrows(IllegalStateException.class, service::getClient);
    }
}
