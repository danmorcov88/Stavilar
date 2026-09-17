# Stavilar

Native ClickHouse bundle for Apache NiFi 2.x, built on the official ClickHouse Java client (`client-v2`).

A *stăvilar* is a sluice gate: it controls how much water flows and when. Here it controls the flow from NiFi into ClickHouse.

**Status: work in progress.** Nothing usable yet. See [NOTES.md](NOTES.md) for what is done and what comes next.

## What it will do

- `ClickHouseConnectionService`: a controller service that holds a `client-v2` client (HTTP, LZ4, failover between endpoints).
- `PutClickHouseRecord`: bulk inserts from any NiFi Record Reader, in `JSONEachRow` or `RowBinary`, with insert deduplication tokens so retries do not create duplicate rows.
- `QueryClickHouseRecord`: streams query results into FlowFiles through a Record Writer, without holding the whole result in memory.
- `ExecuteClickHouseStatement`: runs DDL/DML that returns no rows.

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

## License

Apache License 2.0. See [LICENSE](LICENSE).
