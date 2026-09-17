# Stavilar

Native ClickHouse bundle for Apache NiFi 2.x, built on the official ClickHouse Java client (`client-v2`).

A *stăvilar* is a sluice gate: it controls how much water flows and when. Here it controls the flow from NiFi into ClickHouse.

**Status: work in progress.** The connection service and `PutClickHouseRecord` (JSONEachRow) work. See [NOTES.md](NOTES.md) for what is done and what comes next.

## Components

- `ClickHouseConnectionService`: a controller service that holds a `client-v2` client (HTTP, LZ4, failover between endpoints). Done.
- `PutClickHouseRecord`: bulk inserts from any NiFi Record Reader, with insert deduplication tokens so retries do not create duplicate rows. Done (JSONEachRow; RowBinary comes next).
- `QueryClickHouseRecord`: streams query results into FlowFiles through a Record Writer, without holding the whole result in memory. Planned.
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
| Max Rows Per Insert | 100000 | larger FlowFiles are sent in several inserts; one insert is held in memory at a time |
| Deduplication Token | None | `None`, `FlowFile UUID` or `Attribute` (+ `Deduplication Token Attribute`) |
| Async Insert | false | `async_insert=1`, `wait_for_async_insert=1`; cannot be combined with a token |
| Wait End Of Query | true | `wait_end_of_query=1`: the response confirms the write, including materialized views |
| `ch.setting.<name>` | | any ClickHouse setting, sent with every insert; overrides the same setting on the service |

Relationships: `success`; `failure` for data, schema and table errors (unknown column, bad value, missing table) and unreadable input; `retry` for network problems and server conditions ClickHouse marks retryable (too many parts, memory limit, timeouts). Failed FlowFiles carry the server message in `clickhouse.error`.

Settings the processor sends on its own, all overridable with `ch.setting.*`: `date_time_input_format=best_effort` (timestamps go out as ISO-8601 in UTC), `input_format_skip_unknown_fields=0` (a record field without a column is an error, not silent loss), `async_insert=0` unless Async Insert is on (recent ClickHouse versions default to async inserts).

### Deduplication

- The token is sent as `insert_deduplication_token`. ClickHouse drops an insert whose token it has already seen inside the table's deduplication window, whatever the content.
- A FlowFile that needs several inserts uses `<token>` for the first one and `<token>-<i>` for the following ones, so a retry of the same FlowFile produces the same tokens.
- If insert *n* of a FlowFile fails, inserts before it are already in the table. With a token, a retry is safe: the earlier inserts are dropped as duplicates. Without a token, a retry duplicates them. Use a token whenever a retry loop exists.
- `clickhouse.rows.written` counts the rows ClickHouse accepted in the request; an insert dropped by deduplication still reports its rows.
- Async inserts ignore the token, so the processor refuses that combination.

### Value encoding (JSONEachRow)

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

## License

Apache License 2.0. See [LICENSE](LICENSE).
