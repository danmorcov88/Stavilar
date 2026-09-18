# QueryClickHouseRecord

Runs a query on ClickHouse and writes the result rows with any NiFi Record Writer (Avro, JSON, CSV, Parquet, ...).

## How it works

1. The query comes from `SQL Query` (Expression Language against the incoming FlowFile's attributes) or, when that property is empty, from the content of the incoming FlowFile.
2. ClickHouse answers in `RowBinaryWithNamesAndTypes`. The header gives the column names and types; the record schema is built from it, so no table metadata is read up front and any `SELECT` works.
3. Rows are read from the HTTP stream and written to the FlowFile one at a time. Memory does not grow with the result: a 100M-row result needs no more heap than a 100-row one.
4. With `Max Rows Per FlowFile > 0` the result is split into several FlowFiles. Each gets `fragment.identifier` (shared), `fragment.index` (from 0) and `fragment.count`.

The processor works with or without an incoming FlowFile. It can run on a schedule with a fixed query, or be triggered by FlowFiles that carry the query in an attribute or in their content.

## Relationships

| Relationship | What goes there |
|---|---|
| `success` | the result FlowFiles, with `record.count`, `mime.type` (from the writer), `clickhouse.rows.read`, `clickhouse.query.id` |
| `failure` | the incoming FlowFile, or a new empty FlowFile when there was none, with `clickhouse.error` and `clickhouse.query.id` |
| `original` | the incoming FlowFile after a successful query |

An empty result still produces one FlowFile with `record.count = 0`, so downstream always sees the same shape.

## Result schema

| ClickHouse | NiFi record type |
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
| `DateTime`, `DateTime64` | timestamp, as the UTC instant; the writer decides the text representation |
| `Nullable(T)`, `LowCardinality(T)` | T |
| `Array(T)` | array of T |
| `Map(K, V)` | map of V; keys become strings |
| `Tuple(...)` | record; unnamed elements are `_1`, `_2`, ... |

Every field is nullable in the record schema, whatever the column type, because a `SELECT` can produce NULLs (outer joins, `nullIf`, ...).

## Settings

`ch.setting.<name>` dynamic properties are sent with the query and override the same setting on the connection service. Common ones: `max_execution_time`, `max_result_rows`, `max_block_size`, `max_threads`.

## Examples

Scheduled export, every hour, one FlowFile per million rows:

- `SQL Query`: `SELECT * FROM events WHERE ts >= now() - INTERVAL 1 HOUR`
- `Max Rows Per FlowFile`: `1000000`
- `Record Writer`: `ParquetRecordSetWriter`

Query per incoming FlowFile, table name from an attribute:

- `SQL Query`: `SELECT count() AS rows FROM ${table}`
