package io.github.danmorcov88.stavilar.api;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * Gives processors a configured ClickHouse client-v2 {@code Client}.
 * Methods are added in the connection service phase.
 */
@Tags({"clickhouse", "database", "connection", "client"})
@CapabilityDescription("Provides a shared ClickHouse client to processors.")
public interface ClickHouseConnectionService extends ControllerService {
}
