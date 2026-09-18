package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseRecordSchemaTest {

    @ParameterizedTest
    @CsvSource({
            "Int8, INT", "Int16, INT", "Int32, INT", "UInt8, INT", "UInt16, INT",
            "Int64, LONG", "UInt32, LONG",
            "UInt64, BIGINT", "Int128, BIGINT", "UInt128, BIGINT", "Int256, BIGINT", "UInt256, BIGINT",
            "Float32, FLOAT", "Float64, DOUBLE",
            "String, STRING", "FixedString(3), STRING", "'Enum8(''a'' = 1)', STRING", "IPv4, STRING", "IPv6, STRING", "JSON, STRING",
            "Bool, BOOLEAN", "UUID, UUID", "Date, DATE", "Date32, DATE",
            "DateTime, TIMESTAMP", "'DateTime(''UTC'')', TIMESTAMP", "'DateTime64(3, ''UTC'')', TIMESTAMP",
            "Nullable(Int32), INT", "LowCardinality(String), STRING", "LowCardinality(Nullable(String)), STRING"})
    void mapsScalarTypes(final String clickHouseType, final String expected) {
        assertEquals(RecordFieldType.valueOf(expected), ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", clickHouseType)).getFieldType());
    }

    @Test
    void mapsDecimalWithPrecisionAndScale() {
        final DataType type = ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", "Decimal(18, 4)"));
        assertEquals(RecordFieldType.DECIMAL, type.getFieldType());
        assertEquals(18, ((DecimalDataType) type).getPrecision());
        assertEquals(4, ((DecimalDataType) type).getScale());
    }

    @Test
    void mapsArraysMapsAndTuples() {
        final DataType array = ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", "Array(Array(Nullable(Int64)))"));
        assertEquals(RecordFieldType.ARRAY, array.getFieldType());
        final DataType inner = ((ArrayDataType) array).getElementType();
        assertEquals(RecordFieldType.ARRAY, inner.getFieldType());
        assertEquals(RecordFieldType.LONG, ((ArrayDataType) inner).getElementType().getFieldType());

        final DataType map = ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", "Map(String, Float64)"));
        assertEquals(RecordFieldType.MAP, map.getFieldType());
        assertEquals(RecordFieldType.DOUBLE, ((MapDataType) map).getValueType().getFieldType());

        final DataType tuple = ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", "Tuple(Int32, String)"));
        assertEquals(RecordFieldType.RECORD, tuple.getFieldType());
        assertEquals(List.of("_1", "_2"), ((RecordDataType) tuple).getChildSchema().getFieldNames());

        final DataType named = ClickHouseRecordSchema.dataType(ClickHouseColumn.of("c", "Tuple(x Int32, y String)"));
        assertEquals(List.of("x", "y"), ((RecordDataType) named).getChildSchema().getFieldNames());
    }

    @Test
    void buildsSchemaInColumnOrderWithNullableFields() {
        final RecordSchema schema = ClickHouseRecordSchema.of(new TableSchema(ClickHouseColumn.parse("id UInt64, name String, ts DateTime")));
        assertEquals(List.of("id", "name", "ts"), schema.getFieldNames());
        assertTrue(schema.getField("id").orElseThrow().isNullable());
        assertEquals(RecordFieldType.BIGINT, schema.getField("id").orElseThrow().getDataType().getFieldType());
    }

    @Test
    void convertsReaderValues() throws Exception {
        assertNull(ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Nullable(Int32)"), null));
        assertEquals("ab", ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "FixedString(3)"), "ab\0"));
        assertEquals("b", ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Enum8('a' = 1, 'b' = 2)"), new BinaryStreamReader.EnumValue("b", 2)));
        assertEquals(java.sql.Date.valueOf("2024-03-15"), ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Date"), LocalDate.of(2024, 3, 15)));

        final Instant instant = Instant.parse("2024-03-15T11:45:10.123456789Z");
        final Object ts = ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "DateTime64(9, 'UTC')"), instant.atZone(ZoneId.of("Europe/Bucharest")));
        assertEquals(Timestamp.from(instant), ts);
        assertEquals(123_456_789, ((Timestamp) ts).getNanos());

        assertEquals("1.2.3.4", ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "IPv4"), InetAddress.getByName("1.2.3.4")));
        assertEquals(new BigInteger("18446744073709551615"), ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "UInt64"), new BigInteger("18446744073709551615")));

        final BinaryStreamReader.ArrayValue inner = new BinaryStreamReader.ArrayValue(Object.class, 1);
        inner.set(0, "x\0");
        final BinaryStreamReader.ArrayValue outer = new BinaryStreamReader.ArrayValue(Object.class, 2);
        outer.set(0, inner);
        outer.set(1, new BinaryStreamReader.ArrayValue(Object.class, 0));
        final Object[] array = (Object[]) ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Array(Array(FixedString(2)))"), outer);
        assertArrayEquals(new Object[]{"x"}, (Object[]) array[0]);
        assertArrayEquals(new Object[0], (Object[]) array[1]);

        final Object map = ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Map(String, Date)"), Map.of("k", LocalDate.of(2024, 1, 2)));
        assertEquals(Map.of("k", java.sql.Date.valueOf("2024-01-02")), map);

        final Object tuple = ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "Tuple(Int32, String)"), new Object[]{1, "a"});
        assertInstanceOf(MapRecord.class, tuple);
        assertEquals(1, ((MapRecord) tuple).getValue("_1"));
        assertEquals("a", ((MapRecord) tuple).getValue("_2"));

        assertEquals("{\"a\":1}", ClickHouseValues.toRecordValue(ClickHouseColumn.of("c", "JSON"), Map.of("a", 1)));
    }
}
