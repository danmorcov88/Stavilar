package io.github.danmorcov88.stavilar.processors;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Writes NiFi records as ClickHouse {@code JSONEachRow}: one JSON object per line.
 * <p>
 * Dates and times are written so that ClickHouse reads them the same way whatever
 * the time zone of NiFi or the server: dates as {@code yyyy-MM-dd}, times as
 * {@code HH:mm:ss}, timestamps as ISO-8601 in UTC ({@code 2024-01-01T12:00:00.123Z}).
 * The processor sends {@code date_time_input_format=best_effort} so the ISO form parses.
 */
final class JsonEachRowEncoder implements RowEncoder {

    private static final JsonFactory FACTORY = new JsonFactory();
    private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIME = DateTimeFormatter.ISO_LOCAL_TIME;
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ISO_INSTANT;

    private final JsonGenerator generator;
    private final ZoneId localZone;
    private long rows;

    JsonEachRowEncoder(final OutputStream out) throws IOException {
        this(out, ZoneId.systemDefault());
    }

    JsonEachRowEncoder(final OutputStream out, final ZoneId localZone) throws IOException {
        this.generator = FACTORY.createGenerator(out, JsonEncoding.UTF8);
        this.generator.setRootValueSeparator(null);
        this.localZone = localZone;
    }

    @Override
    public void write(final Record record) throws IOException {
        writeRecord(record);
        generator.writeRaw('\n');
        rows++;
    }

    @Override
    public long getRowCount() {
        return rows;
    }

    void flush() throws IOException {
        generator.flush();
    }

    /** One value as JSON text, for example a Map read from a JSON column. */
    static String toJson(final Object value) throws IOException {
        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (JsonEachRowEncoder encoder = new JsonEachRowEncoder(out)) {
            encoder.writeValue(value);
        }
        return out.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        generator.close();
    }

    private void writeRecord(final Record record) throws IOException {
        generator.writeStartObject();
        for (final String field : record.getRawFieldNames()) {
            generator.writeFieldName(field);
            writeValue(record.getValue(field));
        }
        generator.writeEndObject();
    }

    private void writeValue(final Object value) throws IOException {
        switch (value) {
            case null -> generator.writeNull();
            case String s -> generator.writeString(s);
            case Boolean b -> generator.writeBoolean(b);
            case Integer i -> generator.writeNumber(i);
            case Long l -> generator.writeNumber(l);
            case Short s -> generator.writeNumber(s);
            case Byte b -> generator.writeNumber(b);
            case Double d -> writeDouble(d);
            case Float f -> writeDouble(f);
            case BigDecimal d -> generator.writeNumber(d);
            case BigInteger i -> generator.writeNumber(i);
            case Number n -> generator.writeNumber(n.toString());
            case Character c -> generator.writeString(String.valueOf(c));
            case UUID u -> generator.writeString(u.toString());
            case Enum<?> e -> generator.writeString(e.name());
            // java.sql.Date and java.sql.Time extend java.util.Date; check them first
            case java.sql.Date d -> generator.writeString(DATE.format(d.toLocalDate()));
            case java.sql.Time t -> generator.writeString(TIME.format(t.toLocalTime()));
            case java.util.Date d -> generator.writeString(TIMESTAMP.format(d.toInstant()));
            case LocalDate d -> generator.writeString(DATE.format(d));
            case LocalTime t -> generator.writeString(TIME.format(t));
            case LocalDateTime t -> generator.writeString(TIMESTAMP.format(t.atZone(localZone).toInstant()));
            case OffsetDateTime t -> generator.writeString(TIMESTAMP.format(t.toInstant()));
            case ZonedDateTime t -> generator.writeString(TIMESTAMP.format(t.toInstant()));
            case Instant t -> generator.writeString(TIMESTAMP.format(t));
            case byte[] bytes -> generator.writeString(Base64.getEncoder().encodeToString(bytes));
            case Record r -> writeRecord(r);
            case Map<?, ?> map -> writeMap(map);
            case Collection<?> c -> writeArray(c.toArray());
            case Object[] array -> writeArray(array);
            default -> {
                if (value.getClass().isArray()) {
                    writePrimitiveArray(value);
                } else {
                    generator.writeString(value.toString());
                }
            }
        }
    }

    private void writeDouble(final double d) throws IOException {
        // JSON has no NaN or Infinity; ClickHouse would reject the token.
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            generator.writeNull();
        } else {
            generator.writeNumber(d);
        }
    }

    private void writeMap(final Map<?, ?> map) throws IOException {
        generator.writeStartObject();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            generator.writeFieldName(String.valueOf(entry.getKey()));
            writeValue(entry.getValue());
        }
        generator.writeEndObject();
    }

    private void writeArray(final Object[] array) throws IOException {
        generator.writeStartArray();
        for (final Object element : array) {
            writeValue(element);
        }
        generator.writeEndArray();
    }

    private void writePrimitiveArray(final Object array) throws IOException {
        final int length = java.lang.reflect.Array.getLength(array);
        generator.writeStartArray();
        for (int i = 0; i < length; i++) {
            writeValue(java.lang.reflect.Array.get(array, i));
        }
        generator.writeEndArray();
    }
}
