package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;

import java.util.List;
import java.util.Map;

/**
 * Column list of a table, read from {@code system.columns}. client-v2's own
 * {@code getTableSchema} parses {@code DESCRIBE TABLE} output and breaks on
 * ClickHouse 26.x; {@code system.columns} is stable and also says which columns
 * cannot be inserted into (ALIAS, MATERIALIZED).
 */
final class TableColumns {

    record TableColumn(ClickHouseColumn column, boolean insertable) {
        String name() {
            return column.getColumnName();
        }
    }

    private TableColumns() {
    }

    /**
     * @throws IllegalArgumentException when the table has no columns, that is, it does not exist
     */
    static List<TableColumn> load(final Client client, final String database, final String table) {
        final List<GenericRecord> rows = client.queryAll(
                "SELECT name, type, default_kind FROM system.columns WHERE database = {db:String} AND table = {t:String} ORDER BY position",
                Map.of("db", unquote(database), "t", unquote(table)));
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("table " + database + "." + table + " does not exist or has no columns");
        }
        return rows.stream()
                .map(row -> new TableColumn(
                        ClickHouseColumn.of(row.getString("name"), row.getString("type")),
                        !isComputed(row.getString("default_kind"))))
                .toList();
    }

    private static boolean isComputed(final String defaultKind) {
        return "ALIAS".equals(defaultKind) || "MATERIALIZED".equals(defaultKind);
    }

    /** The Table and Database properties are written as in SQL; system.columns wants bare names. */
    static String unquote(final String identifier) {
        final String s = identifier.trim();
        return s.length() >= 2 && s.startsWith("`") && s.endsWith("`") ? s.substring(1, s.length() - 1) : s;
    }
}
