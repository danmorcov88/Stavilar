# Example flows

Flow definitions exported from Apache NiFi 2.12. Import one with a right-click on the canvas → **Upload flow definition** (or drag the process group icon from the toolbar and pick the file). Each becomes a process group with its own controller services.

Passwords are never part of an export. After importing, open the `ClickHouse` controller service inside the group, set **Password**, check **Endpoints**, and enable the services.

| File | What it does |
|---|---|
| `csv-to-clickhouse.json` | `GenerateFlowFile` (sample CSV, every minute) → `PutClickHouseRecord` (RowBinary, FlowFile UUID as deduplication token) → `LogAttribute`. NiFi retries the `retry` relationship 10 times with backoff; `failure` goes to a `LogAttribute` at error level. |
| `postgres-to-clickhouse.json` | `QueryDatabaseTableRecord` (incremental on `id`, Avro) → `PutClickHouseRecord` with deduplication → `LogAttribute`. Needs a PostgreSQL JDBC driver jar; set its path in the `PostgreSQL` controller service (`Database Driver Location(s)`), plus URL, user and password. |
| `clickhouse-to-json.json` | `QueryClickHouseRecord` (every hour, one FlowFile per 100000 rows) → `JsonRecordSetWriter` → `LogAttribute`. |

## Prerequisites

A ClickHouse to talk to. For a local test:

```
docker run -d --name clickhouse -p 8123:8123 \
  -e CLICKHOUSE_USER=stavilar -e CLICKHOUSE_PASSWORD=stavilar -e CLICKHOUSE_DB=stavilar \
  clickhouse/clickhouse-server:26.8
```

The tables the flows write to or read from:

```sql
-- csv-to-clickhouse, clickhouse-to-json
CREATE TABLE stavilar.events (id UInt32, name String, ts DateTime('UTC'))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000;

-- postgres-to-clickhouse (adjust to the columns of your PostgreSQL table)
CREATE TABLE stavilar.orders (id UInt64, customer String, amount Decimal(18, 2), created DateTime('UTC'))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000;
```

`Endpoints` in the exports is `clickhouse:8123`. If NiFi runs on your machine and ClickHouse in Docker as above, change it to `localhost:8123`; if both run in Docker on one network, use the ClickHouse container name.

## About the deduplication token in the retry loop

`PutClickHouseRecord` sends the FlowFile's `uuid` as `insert_deduplication_token`. When NiFi retries a FlowFile (the `retry` relationship, or your own loop back into the processor), the uuid stays the same, so an insert that already went through before the failure is dropped by ClickHouse instead of being applied twice. This only works on tables with a deduplication window, which is why the `CREATE TABLE` statements above set `non_replicated_deduplication_window` (`ReplicatedMergeTree` tables have one by default).

`GenerateFlowFile` produces a new FlowFile, with a new uuid, every minute, so the sample flow adds three rows per minute. That is the demo working, not deduplication failing.
