# StandardClickHouseConnectionService

Holds one ClickHouse client (the official Java `client-v2`, HTTP interface) and shares it with the Stavilar processors: `PutClickHouseRecord`, `QueryClickHouseRecord`, `ExecuteClickHouseStatement`.

## Why not JDBC

NiFi already ships `PutDatabaseRecord` and `ExecuteSQL` over JDBC. This service exists for what JDBC cannot do well with ClickHouse: stream data in ClickHouse's own formats (`RowBinary`, `RowBinaryWithNamesAndTypes`), pass ClickHouse settings per request, and use insert deduplication tokens so that retries do not duplicate rows.

## Properties

| Property | Meaning |
|---|---|
| Endpoints | Comma-separated `host:port` list of the HTTP interface (port 8123, or 8443 with TLS). With several endpoints the client fails over between them. |
| Use TLS | Connect with HTTPS. |
| SSL Context Service | Only shown with TLS. Its trust store is used to verify the server certificate. Client certificates (mTLS) are not supported: the client library takes PEM files, NiFi provides key stores. Without a service, the JVM's default trust store is used. |
| Database | Default database for queries and inserts that do not name one. |
| Username / Password | ClickHouse user. |
| Compression | `LZ4` compresses request and response bodies; `None` sends plain data. |
| Connect Timeout / Socket Timeout | TCP connect timeout and read timeout. Raise the socket timeout for long inserts or queries. |
| `ch.setting.<name>` | Dynamic properties. Each one is a ClickHouse server setting sent with every request made through this client, for example `ch.setting.max_execution_time = 300`. Processors can override a setting per request. |

## What happens on enable

The service builds the client and runs `SELECT version()`. If that fails (wrong host, wrong password, TLS problem) the service does not enable and the server's message appears in the bulletin. Nothing is cached between enable and disable except the connection pool.

## Settings order

A setting can come from three places; the most specific wins:

1. the server's defaults for the user,
2. `ch.setting.*` on this service,
3. `ch.setting.*` on the processor.

## Notes

- The client name sent to ClickHouse starts with `stavilar`; `system.query_log.http_user_agent` shows which requests came from NiFi.
- One service can serve any number of processors. The client is thread-safe and pools connections.
