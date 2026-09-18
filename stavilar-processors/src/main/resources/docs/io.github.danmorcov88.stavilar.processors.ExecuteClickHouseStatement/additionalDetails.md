# ExecuteClickHouseStatement

Runs one SQL statement that returns no rows: `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `OPTIMIZE`, `INSERT ... SELECT`, `SYSTEM ...`. For statements whose rows matter, use `QueryClickHouseRecord`.

## How it works

1. The statement comes from `SQL Statement` (Expression Language against the incoming FlowFile's attributes) or, when that property is empty, from the content of the incoming FlowFile.
2. It is sent as one request. ClickHouse's HTTP interface runs one statement per request, so do not put several statements separated by `;` in one FlowFile; use one FlowFile per statement or a chain of processors.
3. The `X-ClickHouse-Summary` header of the response becomes attributes named `clickhouse.summary.<key>`: `read_rows`, `read_bytes`, `written_rows`, `written_bytes`, `result_rows`, `result_bytes`, `elapsed_ns`, `memory_usage` and whatever else the server version adds.

The processor works with or without an incoming FlowFile. Without one, a new empty FlowFile carries the attributes.

## Relationships

| Relationship | When |
|---|---|
| `success` | the statement ran; the FlowFile has `clickhouse.query.id` and the summary attributes |
| `failure` | ClickHouse rejected the statement (syntax error, unknown table, missing permission); `clickhouse.error` has the message |
| `retry` | network problem or a server condition ClickHouse marks as retryable; the processor yields |

## Settings

`ch.setting.<name>` dynamic properties are sent with the statement and override the same setting on the connection service. The ones that matter most here:

- `mutations_sync = 1`: wait for `ALTER TABLE ... UPDATE/DELETE` to finish before answering (`2` waits for all replicas). Without it the mutation runs in the background and the next processor may still see the old rows.
- `max_execution_time`: cap long statements such as `INSERT ... SELECT` or `OPTIMIZE`.
- `alter_sync`: same idea for `ALTER` operations on replicated tables.

## Examples

Create a table once at flow start (no incoming connection, run once):

```sql
CREATE TABLE IF NOT EXISTS events (id UInt32, name String, ts DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000
```

Delete rows older than a value from an attribute, and wait for it:

- `SQL Statement`: `ALTER TABLE events DELETE WHERE ts < toDateTime('${cutoff}')`
- `ch.setting.mutations_sync`: `1`

Move data between tables server-side:

```sql
INSERT INTO events_archive SELECT * FROM events WHERE ts < now() - INTERVAL 30 DAY
```

`clickhouse.summary.written_rows` on the outgoing FlowFile says how many rows moved.
