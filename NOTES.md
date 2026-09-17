# Notes

Working notes per phase: what was done, what is left, decisions taken.

## Phase 1 — ClickHouseConnectionService (2026-09-17)

Done:
- `ClickHouseConnectionService` interface: `getClient()`, `getDefaultSettings()`, `getDatabase()`.
- `StandardClickHouseConnectionService`: endpoints with failover, TLS with trust store from an SSL Context Service,
  LZ4 or no compression, connect/socket timeouts, `ch.setting.*` dynamic properties sent with every request.
- On enable the service runs `SELECT version()`; if that fails the service stays disabled and the server's error is in the bulletin.
- Unit tests (validation) and integration tests against `clickhouse/clickhouse-server:26.8` in Testcontainers.
- Checked in a real NiFi 2.12.0 container through the REST API: service enables against a ClickHouse container,
  `system.query_log` shows the client name and the dynamic setting, a wrong port fails with a clear bulletin.

Decisions:
- `getDatabase()` is not in the original property list. Processors need it to fall back to the service's database.
- Enable check is a real query (`SELECT version()`), not `Client.ping()`. `ping()` swallows the error, so a wrong
  password would only say "ping failed". The query reports `AUTHENTICATION_FAILED` with the server's text.
- TLS uses only the trust store of the SSL Context Service (`setSSLTrustStore/Password/Type`). client-v2 has no
  way to take an `SSLContext`, and it wants PEM files for client certificates, so mTLS is out of v1.
- Testcontainers `clickhouse` module is not used: it extends `JdbcDatabaseContainer` and waits for start through JDBC,
  which needs the JDBC driver. A `GenericContainer` with `Wait.forHttp("/ping")` does the job without JDBC.
- client-v2 0.10.0 starts no background threads with the default (synchronous) settings, so "no leftover threads
  after disable" is checked by diffing thread names before enable and after disable.
- `slf4j-simple` is a test dependency so Testcontainers and the service log during tests.

Left for later:
- Client name is `stavilar` without a version in the NAR (no `Implementation-Version` in the jar manifest). Add it if it
  turns out useful for support.
- Connection pool size, retries, proxies: client-v2 defaults, no properties yet.

## Phase 0 — skeleton and CI (2026-09-17)

Done:
- Parent POM `io.github.danmorcov88:stavilar:0.1.0-SNAPSHOT` with six modules; `mvn clean package` produces three NARs.
- GitHub Actions: `build` (unit tests, uploads NARs) and `integration-tests` (`-Pintegration-tests`, needs Docker).
- README, CHANGELOG, this file.

Versions picked (latest at the time, from Maven Central metadata):
- NiFi 2.12.0, `nifi-nar-maven-plugin` 2.4.0
- `com.clickhouse:client-v2` 0.10.0
- JUnit 6.1.3, Testcontainers 1.21.4, ClickHouse image `clickhouse/clickhouse-server:26.8` (LTS)

Decisions:
- `client-v2` is bundled in `stavilar-services-api-nar`, not in the services NAR. The service interface returns the client-v2 `Client`, so the service NAR and the processor NAR must load the same class from the same parent NAR.
- Parent NAR of `stavilar-services-api-nar` is `nifi-standard-services-api-nar`: it provides `SSLContextService`, the Record Reader/Writer APIs and `nifi-record`, which both the service and the processors need.
- JUnit 6 instead of JUnit 5. NiFi 2.12.0 and `nifi-mock` 2.12.0 depend on JUnit 6.1.3; the Jupiter API is the same. Matching NiFi avoids two JUnit versions on the test classpath.
- `slf4j-api` is excluded from the client-v2 bundle; NiFi provides it.
- Jackson is not bundled. `client-v2` marks it `provided` and only needs it for POJO serialization, which this bundle does not use. Revisit in Phase 2a if the JSONEachRow path needs it.
- Integration tests run only under the `integration-tests` profile (failsafe, `*IT` classes), so `mvn verify` stays Docker-free.

Left for later:
- Nothing from Phase 0. Phase 1 adds the connection service.
