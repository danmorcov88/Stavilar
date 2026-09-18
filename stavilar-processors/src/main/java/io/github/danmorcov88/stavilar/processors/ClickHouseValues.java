package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the values client-v2's binary reader returns into values NiFi record
 * writers understand. The reader's {@code ArrayValue} and {@code EnumValue}
 * come from a package named {@code internal}; this is the one place that
 * touches them.
 */
final class ClickHouseValues {

    private ClickHouseValues() {
    }

    static Object toRecordValue(final ClickHouseColumn column, final Object value) {
        if (value == null) {
            return null;
        }
        return switch (column.getDataType()) {
            case FixedString -> stripTrailingNuls(value.toString());
            case Enum8, Enum16 -> value instanceof BinaryStreamReader.EnumValue e ? e.getName() : value.toString();
            case Date, Date32 -> value instanceof LocalDate d ? java.sql.Date.valueOf(d) : value;
            case DateTime, DateTime32, DateTime64 -> value instanceof ZonedDateTime t ? Timestamp.from(t.toInstant()) : value;
            case IPv4, IPv6 -> value instanceof InetAddress a ? a.getHostAddress() : value.toString();
            case Array -> toArray(column.getNestedColumns().get(0), value);
            case Map -> toMap(column.getNestedColumns().get(1), value);
            case Tuple -> toTuple(column, value);
            case JSON, Object, Dynamic, Variant -> value instanceof CharSequence ? value.toString() : toJson(value);
            default -> value;
        };
    }

    private static Object[] toArray(final ClickHouseColumn element, final Object value) {
        final Object[] items;
        if (value instanceof BinaryStreamReader.ArrayValue a) {
            items = a.toObjectArray();
        } else if (value instanceof List<?> l) {
            items = l.toArray();
        } else if (value instanceof Object[] o) {
            items = o;
        } else {
            return new Object[]{toRecordValue(element, value)};
        }
        final Object[] converted = new Object[items.length];
        for (int i = 0; i < items.length; i++) {
            converted[i] = toRecordValue(element, items[i]);
        }
        return converted;
    }

    private static Map<String, Object> toMap(final ClickHouseColumn valueColumn, final Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        final Map<String, Object> converted = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            converted.put(String.valueOf(entry.getKey()), toRecordValue(valueColumn, entry.getValue()));
        }
        return converted;
    }

    private static MapRecord toTuple(final ClickHouseColumn tuple, final Object value) {
        final RecordSchema schema = ClickHouseRecordSchema.tupleSchema(tuple);
        final List<ClickHouseColumn> elements = tuple.getNestedColumns();
        final Object[] items = value instanceof Object[] o ? o : value instanceof List<?> l ? l.toArray() : new Object[]{value};
        final Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < elements.size() && i < items.length; i++) {
            values.put(ClickHouseRecordSchema.tupleElementName(elements.get(i), i), toRecordValue(elements.get(i), items[i]));
        }
        return new MapRecord(schema, values);
    }

    static String stripTrailingNuls(final String s) {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '\0') {
            end--;
        }
        return s.substring(0, end);
    }

    private static String toJson(final Object value) {
        try {
            return JsonEachRowEncoder.toJson(value);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
