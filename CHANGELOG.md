# Changelog

All notable changes to this project are listed here.

## Unreleased

- Project skeleton: Maven modules, three NARs, GitHub Actions CI.
- `ClickHouseConnectionService` controller service: endpoints with failover, TLS trust store, LZ4, timeouts, `ch.setting.*` server settings.
- `PutClickHouseRecord` processor: RowBinary (default) or JSONEachRow inserts from any Record Reader, batching, insert deduplication tokens, async insert option, `success`/`failure`/`retry` routing.
- `QueryClickHouseRecord` processor: streams `RowBinaryWithNamesAndTypes` results into FlowFiles through any Record Writer, optional split with `fragment.*` attributes, query settings per processor.
- `ExecuteClickHouseStatement` processor: DDL/DML without a result set, `X-ClickHouse-Summary` as attributes.
- Component documentation shown in NiFi, three example flows, full README.
