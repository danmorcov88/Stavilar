package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a NiFi record schema from the column list of a query result.
 */
final class ClickHouseRecordSchema {

    private ClickHouseRecordSchema() {
    }

    static RecordSchema of(final TableSchema schema) {
        final List<RecordField> fields = new ArrayList<>();
        for (final ClickHouseColumn column : schema.getColumns()) {
            fields.add(new RecordField(column.getColumnName(), dataType(column), true));
        }
        return new SimpleRecordSchema(fields);
    }

    static DataType dataType(final ClickHouseColumn column) {
        return switch (column.getDataType()) {
            case Int8, Int16, Int32, UInt8, UInt16 -> RecordFieldType.INT.getDataType();
            case Int64, UInt32 -> RecordFieldType.LONG.getDataType();
            case UInt64, Int128, UInt128, Int256, UInt256 -> RecordFieldType.BIGINT.getDataType();
            case Float32 -> RecordFieldType.FLOAT.getDataType();
            case Float64 -> RecordFieldType.DOUBLE.getDataType();
            case Decimal, Decimal32, Decimal64, Decimal128, Decimal256 ->
                    RecordFieldType.DECIMAL.getDecimalDataType(column.getPrecision(), column.getScale());
            case Bool -> RecordFieldType.BOOLEAN.getDataType();
            case UUID -> RecordFieldType.UUID.getDataType();
            case Date, Date32 -> RecordFieldType.DATE.getDataType();
            case DateTime, DateTime32, DateTime64 -> RecordFieldType.TIMESTAMP.getDataType();
            case Array -> RecordFieldType.ARRAY.getArrayDataType(dataType(column.getNestedColumns().get(0)), true);
            case Map -> RecordFieldType.MAP.getMapDataType(dataType(column.getNestedColumns().get(1)));
            case Tuple -> RecordFieldType.RECORD.getRecordDataType(tupleSchema(column));
            // String, FixedString, Enum8/16, IPv4/IPv6, JSON and everything else arrive as text
            default -> RecordFieldType.STRING.getDataType();
        };
    }

    /** Tuple elements keep their names; unnamed elements become _1, _2, ... */
    static RecordSchema tupleSchema(final ClickHouseColumn tuple) {
        final List<RecordField> fields = new ArrayList<>();
        final List<ClickHouseColumn> elements = tuple.getNestedColumns();
        for (int i = 0; i < elements.size(); i++) {
            fields.add(new RecordField(tupleElementName(elements.get(i), i), dataType(elements.get(i)), true));
        }
        return new SimpleRecordSchema(fields);
    }

    static String tupleElementName(final ClickHouseColumn element, final int index) {
        final String name = element.getColumnName();
        return name == null || name.isBlank() ? "_" + (index + 1) : name;
    }
}
