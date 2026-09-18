# PutClickHouseRecord

Inserts the records of a FlowFile into a ClickHouse table through the HTTP interface. Any NiFi Record Reader can feed it (CSV, JSON, Avro, Parquet, ...).

## How an insert works

1. The reader turns the FlowFile into records.
2. Records are encoded in the `Insert Format` and collected into a batch of at most `Max Rows Per Insert` rows. One batch is held in memory at a time.
3. Each batch is one `INSERT INTO <table> ... FORMAT <format>` request with its own query id (`clickhouse.query.id`, comma-separated when there are several).
4. With `Wait End Of Query` on (default), ClickHouse answers only after the data is written, including into materialized views.

Attributes on success: `clickhouse.rows.written` (rows accepted by ClickHouse, summed over the inserts), `clickhouse.inserts`, `clickhouse.query.id`. On failure or retry: `clickhouse.error` with the server's message.

## Insert Format

**RowBinary** (default). The processor reads the table's columns from `system.columns` once per table and caches them. Each FlowFile inserts the columns its records have, in table order: `INSERT INTO t (a, b, c) FORMAT RowBinary`. Table columns the records do not have get their `DEFAULT`. A record field with no column, or matching an `ALIAS`/`MATERIALIZED` column, routes the FlowFile to `failure`. The cache is refreshed after a server error on the table and when a record has a field the cached schema does not know, so `ALTER TABLE ADD COLUMN` needs no restart.

| ClickHouse column | Accepted record values |
|---|---|
| `Int8..Int64`, `UInt8..UInt32` | integer numbers, integer strings, booleans; out of range → failure |
| `UInt64`, `Int128/256`, `UInt128/256` | as above plus `BigInteger`, `BigDecimal`, decimal strings |
| `Float32/64` | numbers, numeric strings |
| `Decimal(P, S)` | `BigDecimal`, numbers, numeric strings; rounded half-up to S; more than P digits → failure |
| `String` | anything via `toString()`; `byte[]` written as is |
| `FixedString(N)` | strings or `byte[]` of at most N bytes, zero-padded |
| `Bool` | booleans, numbers (≠ 0), `true/false/1/0/yes/no` |
| `UUID` | `UUID`, strings |
| `Date`, `Date32` | `LocalDate`, `java.sql.Date`, `yyyy-MM-dd`, timestamps (their UTC date), integers (days since 1970-01-01) |
| `DateTime`, `DateTime64(P)` | `Timestamp`, `Date`, `Instant`, `OffsetDateTime`, `ZonedDateTime`, `LocalDateTime` (JVM zone), ISO-8601 strings (`2024-03-15T11:45:10.123Z`, `2024-03-15 11:45:10`, with offsets), numbers (epoch seconds, fraction allowed) |
| `Enum8/16` | names, numbers, numeric strings |
| `Nullable(T)` | `null` → NULL, otherwise as T |
| `LowCardinality(T)` | as T |
| `Array(T)` | arrays and collections; elements as T |

A `null` for a column that is not `Nullable` becomes the type's default (0, empty string, `1970-01-01`, first enum value), the same thing ClickHouse does for text formats with `input_format_null_as_default`.

Not supported by RowBinary in this version: `Map`, `Tuple`, `Nested`, `JSON`, `Variant`, `IPv4`/`IPv6`, geo types. Such a column routes the FlowFile to `failure` with a message that says to use JSONEachRow.

**JSONEachRow**. Records are written as one JSON object per line and ClickHouse parses them. All column types work. Encoding: numbers, booleans and null as is (`NaN`/`Infinity` become `null`); strings, chars, enums and UUIDs as strings; dates as `yyyy-MM-dd`; times as `HH:mm:ss`; timestamps as ISO-8601 in UTC; `byte[]` as base64; arrays as JSON arrays; maps and nested records as JSON objects. The processor sends `date_time_input_format=best_effort` (so the ISO timestamps parse) and `input_format_skip_unknown_fields=0` (a record field without a column is an error, not silent loss). Both can be overridden with `ch.setting.*`.

## Deduplication

`Deduplication Token` sends the value as `insert_deduplication_token`. ClickHouse drops an insert whose token it has already seen inside the table's deduplication window, whatever the content of the insert. So the same FlowFile sent twice, for example by a retry loop, does not duplicate rows.

The token has an effect only on tables that keep a deduplication window:

```sql
-- plain MergeTree: opt in
CREATE TABLE events (id UInt32, name String, ts DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000;

-- ReplicatedMergeTree: on by default (replicated_deduplication_window, 1000 blocks)
CREATE TABLE events ON CLUSTER c (id UInt32, name String, ts DateTime)
ENGINE = ReplicatedMergeTree ORDER BY id;
```

Rules:

- `FlowFile UUID` uses the FlowFile's `uuid` attribute. NiFi keeps the uuid when a FlowFile is routed back through a retry connection, which is what makes the loop safe.
- `Attribute` takes the token from the named attribute; a missing or empty attribute routes to `failure`.
- A FlowFile that needs several inserts uses `<token>` for the first one and `<token>-<i>` for the following ones. Retrying the same FlowFile produces the same tokens.
- If insert *n* of a FlowFile fails, inserts before it are already in the table. With a token, a retry is safe: the earlier inserts are dropped as duplicates. Without a token a retry duplicates them. Use a token whenever a retry loop exists.
- `clickhouse.rows.written` counts the rows ClickHouse accepted in the request; an insert dropped by deduplication still reports its rows.
- Async inserts ignore the token, so the processor refuses `Async Insert = true` together with a token.

## Async Insert

`Async Insert = true` sends `async_insert=1` and `wait_for_async_insert=1`: ClickHouse buffers small inserts on the server and writes them together. Useful for many small FlowFiles. When off, the processor sends `async_insert=0` explicitly because recent ClickHouse versions default to asynchronous inserts.

## Routing

| Relationship | When |
|---|---|
| `success` | all inserts of the FlowFile were accepted |
| `failure` | ClickHouse rejected the data (unknown column, bad value, missing table), the records could not be read, or the values could not be converted to the column types |
| `retry` | network problem, or a server condition ClickHouse marks as retryable (too many parts, memory limit, timeout). The processor yields. |

## Settings

`ch.setting.<name>` dynamic properties are sent with every insert and override the same setting on the connection service. Common ones: `max_insert_block_size`, `insert_quorum`, `max_execution_time`, `input_format_null_as_default`.
