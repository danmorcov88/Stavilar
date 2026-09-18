# Stavilar

Native ClickHouse bundle for Apache NiFi 2.x, built on the official ClickHouse Java client (`client-v2`).

A *stăvilar* is a sluice gate: it controls how much water flows and when. Here it controls the flow from NiFi into ClickHouse.

**Status: work in progress.** The connection service, `PutClickHouseRecord` and `QueryClickHouseRecord` work. See [NOTES.md](NOTES.md) for what is done and what comes next.

## Components

- `ClickHouseConnectionService`: a controller service that holds a `client-v2` client (HTTP, LZ4, failover between endpoints). Done.
- `PutClickHouseRecord`: bulk inserts from any NiFi Record Reader as RowBinary or JSONEachRow, with insert deduplication tokens so retries do not create duplicate rows. Done.
- `QueryClickHouseRecord`: streams query results into FlowFiles through a Record Writer, without holding the whole result in memory. Done.
- `ExecuteClickHouseStatement`: runs DDL/DML that returns no rows. Planned.

Why not JDBC: NiFi already has `PutDatabaseRecord` over JDBC. This bundle skips JDBC and streams data in ClickHouse's own formats, passes ClickHouse settings per insert, and deduplicates on retry.

## Requirements

- Apache NiFi 2.12.0 or later 2.x
- Java 21
- ClickHouse 24.x or later (integration tests run against the current LTS)

## Build

```
mvn clean verify
```

Integration tests need Docker:

```
mvn verify -Pintegration-tests
```

The build produces three NARs:

```
stavilar-services-api-nar/target/stavilar-services-api-nar-<version>.nar
stavilar-services-nar/target/stavilar-services-nar-<version>.nar
stavilar-nar/target/stavilar-nar-<version>.nar
```

## Install

Copy the three NARs into NiFi's `lib/` directory (or `nar_extensions/` for hot loading) and restart NiFi. The components show up under the tag `clickhouse`.

## Quick start: CSV to ClickHouse

1. Create the table. A deduplication window is what makes retries safe:

   ```sql
   CREATE TABLE events (id UInt32, name String, ts DateTime('UTC'))
   ENGINE = MergeTree ORDER BY id
   SETTINGS non_replicated_deduplication_window = 1000;
   ```

   `Replicated*MergeTree` tables have a deduplication window by default; plain `MergeTree` needs the setting above.

2. Add a `ClickHouseConnectionService` with `Endpoints = host:8123`, user, password and database, and enable it.
3. Add a `CSVReader` (`Schema Access Strategy = Use String Fields From Header`).
4. Add `PutClickHouseRecord` with the service, the reader, `Table = events` and `Deduplication Token = FlowFile UUID`.
5. Send CSV FlowFiles to it. Rows land in the table; the FlowFile gets `clickhouse.rows.written`, `clickhouse.inserts` and `clickhouse.query.id`.

## PutClickHouseRecord

| Property | Default | Meaning |
|---|---|---|
| ClickHouse Connection Service | | the service above |
| Record Reader | | any NiFi Record Reader |
| Database | service database | optional, supports Expression Language |
| Table | | target table, supports Expression Language |
| Insert Format | RowBinary | `RowBinary` (binary, uses the table schema) or `JSONEachRow` (text, parsed by the server) |
| Max Rows Per Insert | 100000 | larger FlowFiles are sent in several inserts; one insert is held in memory at a time |
| Deduplication Token | None | `None`, `FlowFile UUID` or `Attribute` (+ `Deduplication Token Attribute`) |
| Async Insert | false | `async_insert=1`, `wait_for_async_insert=1`; cannot be combined with a token |
| Wait End Of Query | true | `wait_end_of_query=1`: the response confirms the write, including materialized views |
| `ch.setting.<name>` | | any ClickHouse setting, sent with every insert; overrides the same setting on the service |

Relationships: `success`; `failure` for data, schema and table errors (unknown column, bad value, missing table) and unreadable input; `retry` for network problems and server conditions ClickHouse marks retryable (too many parts, memory limit, timeouts). Failed FlowFiles carry the server message in `clickhouse.error`.

Settings the processor sends on its own, all overridable with `ch.setting.*`: `async_insert=0` unless Async Insert is on (recent ClickHouse versions default to async inserts); with JSONEachRow also `date_time_input_format=best_effort` (timestamps go out as ISO-8601 in UTC) and `input_format_skip_unknown_fields=0` (a record field without a column is an error, not silent loss).

### Deduplication

- The token is sent as `insert_deduplication_token`. ClickHouse drops an insert whose token it has already seen inside the table's deduplication window, whatever the content.
- A FlowFile that needs several inserts uses `<token>` for the first one and `<token>-<i>` for the following ones, so a retry of the same FlowFile produces the same tokens.
- If insert *n* of a FlowFile fails, inserts before it are already in the table. With a token, a retry is safe: the earlier inserts are dropped as duplicates. Without a token, a retry duplicates them. Use a token whenever a retry loop exists.
- `clickhouse.rows.written` counts the rows ClickHouse accepted in the request; an insert dropped by deduplication still reports its rows.
- Async inserts ignore the token, so the processor refuses that combination.

### RowBinary (default)

The processor reads the table's columns from `system.columns` once per table and caches them (the cache is refreshed after a server error or when a record has a field the cached schema does not know, so `ALTER TABLE ADD COLUMN` needs no restart). Each FlowFile inserts the columns its records have, in table order: `INSERT INTO t (a, b, c) FORMAT RowBinary`. Table columns the records do not have get their `DEFAULT`. A record field without a column, or matching an `ALIAS`/`MATERIALIZED` column, routes the FlowFile to `failure`.

Type mapping. The left column is the ClickHouse column type; the right column lists the record values accepted for it.

| ClickHouse | Accepted record values |
|---|---|
| `Int8..Int64`, `UInt8..UInt32` | any integer number, integer string, boolean; out of range → failure |
| `UInt64`, `Int128/256`, `UInt128/256` | as above plus `BigInteger`, `BigDecimal`, decimal string |
| `Float32/64` | any number, numeric string |
| `Decimal(P, S)` | `BigDecimal`, any number, numeric string; rounded half-up to S, more than P digits → failure |
| `String` | any value via `toString()`; `byte[]` written as is |
| `FixedString(N)` | string or `byte[]` of at most N bytes, zero-padded |
| `Bool` | boolean, number (≠ 0), `"true"/"false"/"1"/"0"/"yes"/"no"` |
| `UUID` | `UUID`, string |
| `Date`, `Date32` | `LocalDate`, `java.sql.Date`, `"yyyy-MM-dd"`, any timestamp (its UTC date), integer (days since 1970-01-01) |
| `DateTime`, `DateTime64(P)` | `Timestamp`, `Date`, `Instant`, `OffsetDateTime`, `ZonedDateTime`, `LocalDateTime` (JVM zone), ISO-8601 string (`2024-03-15T11:45:10.123Z`, `2024-03-15 11:45:10`, offsets), number (epoch seconds, fraction allowed) |
| `Enum8/16` | name, number, numeric string |
| `Nullable(T)` | `null` → NULL, otherwise as T |
| `LowCardinality(T)` | as T |
| `Array(T)` | `Object[]`, primitive array, `Collection`; elements as T |

`null` for a column that is not `Nullable` becomes the type's default (0, empty string, `1970-01-01`, first enum value), which is what ClickHouse does for text formats with `input_format_null_as_default`.

Not supported by RowBinary in this version: `Map`, `Tuple`, `Nested`, `JSON`, `Variant`, `IPv4`/`IPv6`, geo types. A table with such a column routes to `failure` with a message that says to use `Insert Format = JSONEachRow`.

### JSONEachRow

| NiFi value | JSON |
|---|---|
| numbers, booleans, null | as is (`NaN`/`Infinity` become `null`) |
| strings, char, enum, UUID | string |
| Date / LocalDate | `"2024-03-15"` |
| Time / LocalTime | `"13:45:10"` |
| Timestamp, Instant, LocalDateTime (JVM zone), OffsetDateTime | `"2024-03-15T11:45:10.123Z"` (UTC) |
| byte[] | base64 string |
| arrays, collections | JSON array |
| maps, nested records | JSON object |

## QueryClickHouseRecord

Runs a query and writes the rows with any Record Writer (Avro, JSON, CSV, Parquet, ...). The response comes as `RowBinaryWithNamesAndTypes` and is written row by row, so a 100M-row result needs no more heap than a 100-row one.

| Property | Default | Meaning |
|---|---|---|
| ClickHouse Connection Service | | the service |
| SQL Query | | the query, supports Expression Language; empty means the query is the content of the incoming FlowFile |
| Record Writer | | any NiFi Record Writer |
| Max Rows Per FlowFile | 0 | 0 = one FlowFile; otherwise the result is split and each FlowFile gets `fragment.identifier`, `fragment.index`, `fragment.count` |
| `ch.setting.<name>` | | query settings such as `max_execution_time`; overrides the service |

The processor works with or without an incoming FlowFile. Relationships: `success` (result FlowFiles, with `record.count`, `mime.type`, `clickhouse.rows.read`, `clickhouse.query.id`), `failure` (the incoming FlowFile, or a new empty one, with `clickhouse.error`), `original` (the incoming FlowFile after a successful query). An empty result still produces one FlowFile with `record.count = 0`.

Result schema, ClickHouse type → NiFi record type:

| ClickHouse | NiFi |
|---|---|
| `Int8`, `Int16`, `Int32`, `UInt8`, `UInt16` | int |
| `Int64`, `UInt32` | long |
| `UInt64`, `Int128`, `UInt128`, `Int256`, `UInt256` | bigint |
| `Float32` / `Float64` | float / double |
| `Decimal(P, S)` | decimal(P, S) |
| `String`, `FixedString` (trailing zero bytes removed), `Enum8/16` (the name), `IPv4/IPv6`, `JSON` (as text) | string |
| `Bool` | boolean |
| `UUID` | uuid |
| `Date`, `Date32` | date |
| `DateTime`, `DateTime64` | timestamp (UTC instant) |
| `Nullable(T)`, `LowCardinality(T)` | T |
| `Array(T)` | array of T |
| `Map(K, V)` | map of V (keys become strings) |
| `Tuple(...)` | record; unnamed elements are `_1`, `_2`, ... |

Measured in the integration test: 5,000,000 rows (`UInt64, String`) read into 5 Avro FlowFiles of 1,000,000 rows, 73 MB in total, in 2.3 s with a 512 MB heap.

### Benchmark

1,000,000 rows from a CSV FlowFile (`id UInt64, name String, ts DateTime64(3), amount Decimal(18,4)`, all values arrive as strings from `CSVReader`), 10 inserts of 100,000 rows, ClickHouse 26.8 in Docker on the same machine (Windows 10, JDK 21), measured inside the integration test:

| Insert Format | Time |
|---|---|
| RowBinary | 1.0 s |
| JSONEachRow | 1.4 s |

Reading the CSV alone takes 0.5 s of that. Run `mvn verify -Pintegration-tests` to see the numbers for your machine.

## License

Apache License 2.0. See [LICENSE](LICENSE).
