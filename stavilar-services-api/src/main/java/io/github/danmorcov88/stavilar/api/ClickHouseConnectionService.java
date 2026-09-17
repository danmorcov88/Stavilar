package io.github.danmorcov88.stavilar.api;

import com.clickhouse.client.api.Client;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

import java.util.Map;

/**
 * Gives processors a configured ClickHouse client-v2 {@link Client}.
 * The client is shared between all processors that use the service. It is
 * created when the service is enabled and closed when it is disabled.
 */
@Tags({"clickhouse", "database", "connection", "client"})
@CapabilityDescription("Provides a shared ClickHouse client to processors.")
public interface ClickHouseConnectionService extends ControllerService {

    /**
     * @return the shared client. Callers must not close it.
     * @throws IllegalStateException if the service is not enabled
     */
    Client getClient();

    /**
     * Server settings configured on the service through dynamic properties
     * ({@code ch.setting.<name>}). The client already sends them with every
     * request; processors use this map to see them and to lay their own
     * settings on top.
     *
     * @return unmodifiable map of setting name to value, in configuration order
     */
    Map<String, String> getDefaultSettings();

    /**
     * @return the default database configured on the service
     */
    String getDatabase();
}
