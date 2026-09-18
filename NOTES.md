# Notes

Working notes per phase: what was done, what is left, decisions taken.

## Phase 3 — QueryClickHouseRecord (2026-09-18)

Done:
- `QueryClickHouseRecord`: SQL from property (EL) or FlowFile content, `RowBinaryWithNamesAndTypes` through client-v2's
  binary reader, any Record Writer, optional split with `fragment.*`, `ch.setting.*` per query, `success`/`failure`/`original`.
- `ClickHouseRecordSchema` (column types → NiFi record schema) and `ClickHouseValues` (reader values → record values).
- Integration tests: all types through Avro and back, empty result, 5M rows split into 5 FlowFiles under `-Xmx512m`
  (73 MB Avro in 2.3 s), SQL error, query from content with `original`, EL in the query, settings in `query_log`, JSON writer.
- Real NiFi 2.12.0 through REST: 2500 rows → 3 JSON FlowFiles (1000/1000/500) with fragment attributes, no log errors.
- `InsertErrors` renamed to `ClickHouseErrors`; both processors use it.

Decisions:
- Streaming: `session.write(flowFile)` gives an OutputStream that stays open while rows arrive; the reader pulls from the
  HTTP stream. One row is live at a time. The failsafe JVM runs with `-Xmx512m` so a regression would fail the build.
- `fragment.count` is only known at the end, so it is set on all fragments after the loop.
- The reader returns `ArrayValue` and `EnumValue` from `com.clickhouse.client.api.data_formats.internal`. They are public
  but the package is named "internal"; only `ClickHouseValues` touches them, so a client-v2 change stays local.
- `DateTime` values arrive as `ZonedDateTime` in the column's zone and are written as `java.sql.Timestamp` instants, so the
  Record Writer decides the representation. `FixedString` loses its trailing NUL padding.
- `Tuple` → record with element names, `_1`, `_2`, ... when unnamed. `Map` keys become strings (NiFi maps are string-keyed).
  `JSON`/`Variant`/`Dynamic` → JSON text through jackson-core.
- No incoming FlowFile and no `SQL Query`: log + yield. This cannot be validated statically; NiFi has no way to tell at
  validation time whether an upstream connection exists.
- An empty result still gives one FlowFile with 0 records, so downstream sees a predictable shape.

Things learned:
- A bare literal like `-170141183460469231731687303715884105728` overflows while the server parses it; the first probe
  blamed the client for a wrong Int128 read. `toInt128('...')` is the right way to write such constants in tests.
- Setting a property on an enabled mock controller service throws; disable, set, enable.

Left for later:
- Query parameters (`{name:Type}`), incremental/state-tracking queries.

## Phase 2b — RowBinary (2026-09-18)

Done:
- `RowBinaryEncoder`: all v1 types (`Int8..Int256`, `UInt8..UInt256`, `Float32/64`, `Decimal`, `String`, `FixedString`,
  `Bool`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Enum8/16`, `Nullable`, `LowCardinality`, `Array`), with
  value coercion from whatever a NiFi reader hands back (typed objects or strings).
- 47 byte-level fixtures generated with `clickhouse-local` from the 26.8 image (`src/test/resources/rowbinary/`,
  `generate.sh` regenerates them); every fixture is checked from several Java input kinds.
- `Insert Format` property on `PutClickHouseRecord`, default `RowBinary`; JSONEachRow kept.
- Table columns cached per target; column list per FlowFile from the record schema; missing columns get DEFAULT.
- Integration tests for both formats (1M rows, all types, unknown column/table), plus RowBinary-only: DEFAULT fill,
  schema refresh after `ALTER TABLE ADD COLUMN`, `Map` column → failure with the JSONEachRow hint.
- Real NiFi 2.12.0 through REST: `INSERT INTO events (id, name, ts) FORMAT RowBinary`, `loaded DateTime DEFAULT now()`
  filled by the server, two loads with the same token → no duplicates, no log errors.

Benchmark (same machine, ClickHouse 26.8 in Docker, 1M CSV rows, 10 inserts): RowBinary 1.0 s, JSONEachRow 1.4 s.
Reading the CSV is 0.5 s of both.

Decisions:
- Column list instead of `RowBinaryWithDefaults`. `INSERT INTO t (a, b) FORMAT RowBinary` works on every version and
  is what a user would write; `RowBinaryWithDefaults` needs a flag byte per value.
- Table schema comes from `system.columns`, not `Client.getTableSchema()`. client-v2 0.10.0 parses `DESCRIBE TABLE`
  and fails on ClickHouse 26.8 ("Non-null columnName and columnType are required"). `system.columns` also says which
  columns are ALIAS/MATERIALIZED; a record field for such a column is rejected instead of failing on the server.
- The schema cache is refreshed in two cases: after a server error on the table, and when a record field is unknown
  to the cached schema (one refresh, then the error stands). So `ALTER TABLE ADD COLUMN` works without a restart.
- `null` for a non-Nullable column writes the type default (0, "", 1970-01-01, first enum value). Same as ClickHouse's
  `input_format_null_as_default` for text formats, so both formats behave alike.
- The encoding is resolved on the first record, so an empty FlowFile makes no request and an unreadable one does not
  touch the network (matters for the failure/retry routing).
- `isRetryable` walks the whole cause chain: client-v2 wraps a transport failure in a generic
  `ClientException("Failed to get query response")` on the query path used for the schema.
- Timestamps as strings go through a hand-written ISO parser (`yyyy-MM-dd[T ]HH:mm:ss[.f][Z|±HH:mm]`) with java.time as
  fallback; `OffsetDateTime.parse` alone cost ~0.7 s per million rows. Same for decimals: `BigDecimal.precision()` and a
  long fast path for Decimal32/64. Without these RowBinary was slower than JSONEachRow from CSV.
- `LocalDateTime` and ISO strings without offset are taken in the JVM zone, as in the JSON path and in NiFi generally.

Left for later:
- Map, Tuple, Nested, IPv4/IPv6, JSON, Variant in RowBinary (brief: optional after v1).
- `Time`/`Time64` column types (new in ClickHouse 25.x) are not mapped.

## Phase 2a — PutClickHouseRecord, JSONEachRow (2026-09-17)

Done:
- `PutClickHouseRecord`: Record Reader → JSONEachRow → `client.insert`, batches of `Max Rows Per Insert`, deduplication
  token (`None` / `FlowFile UUID` / `Attribute`), async insert option, `wait_end_of_query`, `ch.setting.*` per insert.
- `JsonEachRowEncoder` on `jackson-core` (streaming writer only), unit tested per Java type.
- `ClickHouseSettings` in services-api now holds the dynamic property descriptor and the reader for `ch.setting.*`;
  the service and the processor share it.
- Integration tests: 1M rows from CSV in 10 inserts (~1.4 s locally), dedup on retry, indexed tokens for multi-insert
  FlowFiles, unknown column and unknown table → failure, async insert, processor setting overrides service setting,
  all v1 types round trip.
- Real NiFi 2.12.0 through REST: GenerateFlowFile (CSV) → CSVReader → PutClickHouseRecord with an attribute token,
  two loads with the same token, 3 rows in the table, both inserts in `query_log` with the token; no log errors.

Decisions:
- `jackson-core` 2.22.0 is bundled in `stavilar-nar`. The NAR chain has no Jackson, so no clash. No `jackson-databind`.
- One insert per batch, batch buffered in memory (`ByteArrayOutputStream`). The client can only retry a transport
  failure when it can re-read the body, and a token per insert needs a fixed batch boundary. Memory cost is one batch.
- `input_format_skip_unknown_fields=0` is sent by default. ClickHouse's default (1) silently drops record fields that
  have no column; the brief wants a missing column to be a failure.
- `date_time_input_format=best_effort` is sent by default so ISO-8601 UTC timestamps parse into DateTime/DateTime64.
  Checked: `basic` rejects `2024-03-15T11:45:10.123Z`.
- Token per insert: first insert `<token>`, then `<token>-<i>`. Deterministic across retries, no need to know the
  number of inserts up front.
- Error routing uses `ServerException.isRetryable()` plus the transport exceptions. Note the client-v2 hierarchy:
  `ClientException` is only client-side problems; `ConnectionInitiationException` / `TransportException` /
  `DataTransferException` extend `ClickHouseException` directly.
- Query id per insert is generated by the processor and sent with `setQueryId`, so it is known even when the insert fails.

Verified against the brief's risks:
- Risk 1 (`InsertSettings` ignored): not the case on client-v2 0.10.0. `insert_deduplication_token`, `max_threads`,
  `async_insert` sent through `InsertSettings.serverSetting` show up in `system.query_log`.
- Risk 2 (async by default): confirmed on ClickHouse 26.8. A plain insert without `async_insert=0` runs through
  `WaitForAsyncInsert`. The processor always sends `async_insert=0` when Async Insert is off.

Things learned that shape the tests:
- `query_log.Settings` lists only settings that differ from the user's defaults, and format settings
  (`date_time_input_format`, `input_format_*`) and HTTP parameters (`wait_end_of_query`) are not in it.
  Async is verified through `system.asynchronous_insert_log`.
- `X-ClickHouse-Summary.written_rows` still counts the rows of an insert dropped by deduplication.
- `MockRecordParser` replays its records on every run.
- Testcontainers `clickhouse` module needs JDBC; `nifi-mock-record-utils` holds `MockRecordParser`;
  `CSVReader` properties are `Schema Access Strategy` / `Treat First Line as Header`.

Left for later:
- RowBinary insert format (Phase 2b), `Insert Format` property.
- `byte[]` goes out as base64; fine for String columns holding base64, wrong for raw binary. RowBinary fixes this.
- Client name has no version (see Phase 1).

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
