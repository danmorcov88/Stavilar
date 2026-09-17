package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import io.github.danmorcov88.stavilar.api.ClickHouseConnectionService;
import org.apache.nifi.controller.AbstractControllerService;

import java.util.Map;

/**
 * Connection service for unit tests. The client points at a closed port, so any
 * request fails with a connection error and no server is needed.
 */
final class StubConnectionService extends AbstractControllerService implements ClickHouseConnectionService {

    private final Client client = new Client.Builder()
            .addEndpoint("http://localhost:1")
            .setUsername("default")
            .setPassword("")
            .setConnectTimeout(500)
            .setMaxRetries(0)
            .build();

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public Map<String, String> getDefaultSettings() {
        return Map.of();
    }

    @Override
    public String getDatabase() {
        return "default";
    }
}
