package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Picks the table columns a record schema fills, in table order. Columns the
 * record does not have are left out of the insert so the server applies their
 * DEFAULT. A record field without a column is an error, as with JSONEachRow.
 */
final class ColumnMapping {

    static final Set<ClickHouseDataType> SUPPORTED = EnumSet.of(
            ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
            ClickHouseDataType.Int128, ClickHouseDataType.Int256,
            ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32, ClickHouseDataType.UInt64,
            ClickHouseDataType.UInt128, ClickHouseDataType.UInt256,
            ClickHouseDataType.Float32, ClickHouseDataType.Float64,
            ClickHouseDataType.Decimal, ClickHouseDataType.Decimal32, ClickHouseDataType.Decimal64,
            ClickHouseDataType.Decimal128, ClickHouseDataType.Decimal256,
            ClickHouseDataType.String, ClickHouseDataType.FixedString, ClickHouseDataType.Bool, ClickHouseDataType.UUID,
            ClickHouseDataType.Date, ClickHouseDataType.Date32, ClickHouseDataType.DateTime, ClickHouseDataType.DateTime32,
            ClickHouseDataType.DateTime64, ClickHouseDataType.Enum8, ClickHouseDataType.Enum16, ClickHouseDataType.Array);

    private ColumnMapping() {
    }

    /**
     * @throws IllegalArgumentException for a record field without a column, or a column of an unsupported type
     */
    static List<ClickHouseColumn> forRecordSchema(final RecordSchema recordSchema, final TableSchema table, final String tableName) {
        final Set<String> fields = new HashSet<>(recordSchema.getFieldNames());
        final List<ClickHouseColumn> columns = new ArrayList<>();
        for (final ClickHouseColumn column : table.getColumns()) {
            if (fields.remove(column.getColumnName())) {
                checkSupported(column);
                columns.add(column);
            }
        }
        if (!fields.isEmpty()) {
            throw new IllegalArgumentException("record fields " + fields + " have no column in table " + tableName);
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("no record field matches a column of table " + tableName);
        }
        return columns;
    }

    static void checkSupported(final ClickHouseColumn column) {
        ClickHouseColumn current = column;
        while (current.getDataType() == ClickHouseDataType.Array) {
            current = current.getNestedColumns().get(0);
        }
        if (!SUPPORTED.contains(current.getDataType())) {
            throw new IllegalArgumentException("column '" + column.getColumnName() + "' has type " + column.getOriginalTypeName()
                    + ", not supported by RowBinary in this version; use Insert Format = JSONEachRow");
        }
    }
}
