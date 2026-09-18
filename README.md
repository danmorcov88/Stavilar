# Stavilar

Native ClickHouse bundle for Apache NiFi 2.x, built on the official ClickHouse Java client (`client-v2`).

A *stăvilar* is a sluice gate: it controls how much water flows and when. Here it controls the flow between NiFi and ClickHouse.

## What it does

| Component | Purpose |
|---|---|
| `ClickHouseConnectionService` | Holds a `client-v2` client (HTTP, LZ4, failover between endpoints) and shares it with the processors. |
| `PutClickHouseRecord` | Inserts records from any Record Reader as `RowBinary` (default) or `JSONEachRow`, in batches, with insert deduplication tokens so retries do not create duplicate rows. |
| `QueryClickHouseRecord` | Runs a query and streams the result into FlowFiles through any Record Writer; memory does not grow with the result. |
| `ExecuteClickHouseStatement` | Runs DDL/DML that returns no rows (`CREATE`, `ALTER`, `OPTIMIZE`, `INSERT ... SELECT`); the server's summary becomes attributes. |

Why not JDBC: NiFi already has `PutDatabaseRecord` and `ExecuteSQL` over JDBC. This bundle skips JDBC and does what JDBC cannot do well with ClickHouse: stream data in ClickHouse's own binary formats, pass ClickHouse settings per request, and deduplicate on retry.

Each component has its full documentation inside NiFi ("View documentation") and in this repo: [connection service](stavilar-services/src/main/resources/docs/io.github.danmorcov88.stavilar.service.StandardClickHouseConnectionService/additionalDetails.md), [PutClickHouseRecord](stavilar-processors/src/main/resources/docs/io.github.danmorcov88.stavilar.processors.PutClickHouseRecord/additionalDetails.md), [QueryClickHouseRecord](stavilar-processors/src/main/resources/docs/io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord/additionalDetails.md), [ExecuteClickHouseStatement](stavilar-processors/src/main/resources/docs/io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement/additionalDetails.md).

## Requirements

- Apache NiFi 2.12.0 or a later 2.x
- Java 21
- ClickHouse 24.x or later (tested against 26.8, the current LTS)

## Install

Download the three NARs from the [latest release](https://github.com/danmorcov88/Stavilar/releases) (or build them, see below) and copy them into NiFi's `lib/` directory:

```
stavilar-services-api-nar-<version>.nar
stavilar-services-nar-<version>.nar
stavilar-nar-<version>.nar
```

Restart NiFi. The components show up under the tag `clickhouse`. NiFi's `nar_extensions/` directory works too and needs no restart.

## Quick start (15 minutes)

1. Start ClickHouse and create a table:

   ```
   docker run -d --name clickhouse -p 8123:8123 \
     -e CLICKHOUSE_USER=stavilar -e CLICKHOUSE_PASSWORD=stavilar -e CLICKHOUSE_DB=stavilar \
     clickhouse/clickhouse-server:26.8
   ```

   ```sql
   CREATE TABLE stavilar.events (id UInt32, name String, ts DateTime('UTC'))
   ENGINE = MergeTree ORDER BY id
   SETTINGS non_replicated_deduplication_window = 1000;
   ```

   (`docker exec -it clickhouse clickhouse-client --user stavilar --password stavilar` opens a client.)

2. Install the NARs as above and open NiFi.

3. Right-click the canvas → **Upload flow definition** → pick [examples/csv-to-clickhouse.json](examples/csv-to-clickhouse.json).

4. Enter the new process group. Open the `ClickHouse` controller service: set **Endpoints** to `localhost:8123` (or the host NiFi can reach ClickHouse on) and **Password** to `stavilar`. Enable both controller services.

5. Start the process group. Every minute three rows arrive:

   ```sql
   SELECT count() FROM stavilar.events;
   ```

   The `PutClickHouseRecord` output FlowFiles carry `clickhouse.rows.written`, `clickhouse.inserts` and `clickhouse.query.id`; `system.query_log` shows the inserts with `insert_deduplication_token` set.

The other examples ([PostgreSQL → ClickHouse with deduplication](examples/postgres-to-clickhouse.json), [ClickHouse → JSON](examples/clickhouse-to-json.json)) are described in [examples/README.md](examples/README.md).

## Deduplication in one paragraph

`PutClickHouseRecord` can send a token per insert (`FlowFile UUID` or an attribute). On a table with a deduplication window (`ReplicatedMergeTree` by default, `MergeTree` with `non_replicated_deduplication_window > 0`) ClickHouse drops an insert whose token it has already seen. So a FlowFile that is retried after a network error or a partial failure never duplicates rows. The processor refuses the token together with async inserts, because ClickHouse ignores it there. Details and the multi-insert token scheme are in the [processor docs](stavilar-processors/src/main/resources/docs/io.github.danmorcov88.stavilar.processors.PutClickHouseRecord/additionalDetails.md).

## Type mapping

Writing (record value → ClickHouse column, RowBinary):

| ClickHouse | Accepted record values |
|---|---|
| integers (`Int8..Int256`, `UInt8..UInt256`) | numbers, integer strings, booleans; range checked |
| `Float32/64`, `Decimal(P,S)` | numbers, numeric strings; decimals rounded half-up to S |
| `String`, `FixedString(N)` | anything as text; `byte[]` as is |
| `Bool`, `UUID`, `Enum8/16` | booleans / UUID or string / name or number |
| `Date`, `Date32`, `DateTime`, `DateTime64` | `java.time` and `java.sql` types, ISO-8601 strings, epoch numbers |
| `Nullable(T)`, `LowCardinality(T)`, `Array(T)` | as T; arrays and collections |

Reading (ClickHouse column → record type):

| ClickHouse | NiFi |
|---|---|
| `Int8..Int32`, `UInt8`, `UInt16` / `Int64`, `UInt32` / wider integers | int / long / bigint |
| `Float32` / `Float64` / `Decimal(P,S)` | float / double / decimal |
| `String`, `FixedString`, `Enum`, `IPv4/6`, `JSON` | string |
| `Bool`, `UUID`, `Date*`, `DateTime*` | boolean, uuid, date, timestamp |
| `Array`, `Map`, `Tuple` | array, map, record |

Full tables, with the rules for nulls and time zones, are in the component docs.

## Known limitations

- TLS uses the trust store of an SSL Context Service; client certificates (mTLS) are not supported by the client library.
- RowBinary inserts do not cover `Map`, `Tuple`, `Nested`, `JSON`, `Variant`, `IPv4/IPv6`; switch `Insert Format` to `JSONEachRow` for such tables. `Time`/`Time64` columns are not mapped.
- `clickhouse.rows.written` counts rows accepted by the request; an insert dropped by deduplication still reports its rows.
- `ExecuteClickHouseStatement` runs one statement per FlowFile; `;`-separated scripts are not split.
- No query parameters (`{name:Type}`) and no incremental state in `QueryClickHouseRecord`; use Expression Language for dynamic queries.

## Benchmark

1,000,000 rows from a CSV FlowFile (`id UInt64, name String, ts DateTime64(3), amount Decimal(18,4)`, all values arrive as strings from `CSVReader`), 10 inserts of 100,000 rows, ClickHouse 26.8 in Docker on the same machine (Windows 10, JDK 21), measured inside the integration test:

| Insert Format | Time |
|---|---|
| RowBinary | 1.0 s |
| JSONEachRow | 1.4 s |

Reading the CSV alone takes 0.5 s of that. On the read side, 5,000,000 rows (`UInt64, String`) stream into 5 Avro FlowFiles (73 MB) in 2.3 s with a 512 MB heap. `mvn verify -Pintegration-tests` prints the numbers for your machine.

## Build

```
mvn clean verify                      # unit tests
mvn verify -Pintegration-tests        # + integration tests, needs Docker
```

The NARs land in `*/target/`. Integration tests run against `clickhouse/clickhouse-server:26.8` in Testcontainers; `-Dclickhouse.image=...` picks another image.

## License

Apache License 2.0. See [LICENSE](LICENSE).
