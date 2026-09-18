package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseEnum;
import org.apache.nifi.serialization.record.Record;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Writes NiFi records in ClickHouse {@code RowBinary}, one value per column in the
 * order given, using the column types from the table schema.
 * <p>
 * A {@code null} for a column that is not {@code Nullable} becomes the type's default
 * value (0, empty string, 1970-01-01, first enum value), the same thing ClickHouse does
 * for text formats with {@code input_format_null_as_default}.
 */
final class RowBinaryEncoder implements RowEncoder {

    private static final long[] POW10 = {1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L};
    private static final BigInteger UINT64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private final OutputStream out;
    private final List<ClickHouseColumn> columns;
    private final ZoneId localZone;
    private long rows;

    RowBinaryEncoder(final OutputStream out, final List<ClickHouseColumn> columns) {
        this(out, columns, ZoneId.systemDefault());
    }

    RowBinaryEncoder(final OutputStream out, final List<ClickHouseColumn> columns, final ZoneId localZone) {
        this.out = out instanceof BufferedOutputStream ? out : new BufferedOutputStream(out, 64 * 1024);
        this.columns = columns;
        this.localZone = localZone;
    }

    @Override
    public void write(final Record record) throws IOException {
        for (final ClickHouseColumn column : columns) {
            writeValue(out, column, record.getValue(column.getColumnName()), localZone);
        }
        rows++;
    }

    @Override
    public long getRowCount() {
        return rows;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    /**
     * Writes one value in RowBinary.
     *
     * @throws IllegalArgumentException when the value cannot be converted to the column type
     */
    static void writeValue(final OutputStream out, final ClickHouseColumn column, final Object value, final ZoneId localZone) throws IOException {
        try {
            if (column.isNullable()) {
                if (value == null) {
                    out.write(1);
                    return;
                }
                out.write(0);
            }
            writeNonNull(out, column, value, localZone);
        } catch (final IllegalArgumentException | ArithmeticException | DateTimeParseException e) {
            throw new IllegalArgumentException("column '" + column.getColumnName() + "' (" + column.getOriginalTypeName() + "): "
                    + e.getMessage() + (value == null ? "" : " [value of type " + value.getClass().getSimpleName() + "]"), e);
        }
    }

    private static void writeNonNull(final OutputStream out, final ClickHouseColumn column, final Object value, final ZoneId zone) throws IOException {
        final ClickHouseDataType type = column.getDataType();
        switch (type) {
            case Int8 -> out.write((int) toLong(value, -128, 127));
            case UInt8 -> out.write((int) toLong(value, 0, 255));
            case Int16 -> writeShortLE(out, (int) toLong(value, Short.MIN_VALUE, Short.MAX_VALUE));
            case UInt16 -> writeShortLE(out, (int) toLong(value, 0, 65535));
            case Int32 -> writeIntLE(out, (int) toLong(value, Integer.MIN_VALUE, Integer.MAX_VALUE));
            case UInt32 -> writeIntLE(out, (int) toLong(value, 0, 4294967295L));
            case Int64 -> writeLongLE(out, toLong(value, Long.MIN_VALUE, Long.MAX_VALUE));
            case UInt64 -> writeBigIntegerLE(out, toBigInteger(value), 8, BigInteger.ZERO, UINT64_MAX);
            case Int128 -> writeBigIntegerLE(out, toBigInteger(value), 16, BigInteger.ONE.shiftLeft(127).negate(), BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE));
            case UInt128 -> writeBigIntegerLE(out, toBigInteger(value), 16, BigInteger.ZERO, BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE));
            case Int256 -> writeBigIntegerLE(out, toBigInteger(value), 32, BigInteger.ONE.shiftLeft(255).negate(), BigInteger.ONE.shiftLeft(255).subtract(BigInteger.ONE));
            case UInt256 -> writeBigIntegerLE(out, toBigInteger(value), 32, BigInteger.ZERO, BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE));
            case Float32 -> writeIntLE(out, Float.floatToIntBits((float) toDouble(value)));
            case Float64 -> writeLongLE(out, Double.doubleToLongBits(toDouble(value)));
            case Decimal, Decimal32, Decimal64, Decimal128, Decimal256 -> writeDecimal(out, column, value);
            case String -> writeString(out, toBytes(value));
            case FixedString -> writeFixedString(out, toBytes(value), column.getPrecision());
            case Bool -> out.write(toBoolean(value) ? 1 : 0);
            case UUID -> writeUuid(out, toUuid(value));
            case Date -> writeShortLE(out, (int) checkRange(toEpochDay(value, zone), 0, 65535, "Date"));
            case Date32 -> writeIntLE(out, (int) checkRange(toEpochDay(value, zone), Integer.MIN_VALUE, Integer.MAX_VALUE, "Date32"));
            case DateTime, DateTime32 -> writeIntLE(out, (int) checkRange(toInstant(value, zone).getEpochSecond(), 0, 4294967295L, "DateTime"));
            case DateTime64 -> writeLongLE(out, toDateTime64(toInstant(value, zone), column.getScale()));
            case Enum8 -> out.write(toEnumValue(value, column.getEnumConstants(), -128, 127));
            case Enum16 -> writeShortLE(out, toEnumValue(value, column.getEnumConstants(), Short.MIN_VALUE, Short.MAX_VALUE));
            case Array -> writeArray(out, column.getNestedColumns().get(0), value, zone);
            default -> throw new IllegalArgumentException("type " + column.getOriginalTypeName()
                    + " is not supported by RowBinary in this version; use Insert Format = JSONEachRow");
        }
    }

    // ---- conversions ---------------------------------------------------------------------------

    private static long toLong(final Object value, final long min, final long max) {
        final long v;
        if (value == null) {
            v = 0;
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            v = ((Number) value).longValue();
        } else if (value instanceof BigInteger b) {
            v = b.longValueExact();
        } else if (value instanceof BigDecimal d) {
            v = d.toBigIntegerExact().longValueExact();
        } else if (value instanceof Number n) {
            final double d = n.doubleValue();
            if (d != Math.rint(d)) {
                throw new IllegalArgumentException("value " + d + " is not an integer");
            }
            v = (long) d;
        } else if (value instanceof Boolean b) {
            v = b ? 1 : 0;
        } else if (value instanceof CharSequence s) {
            v = new BigDecimal(s.toString().trim()).toBigIntegerExact().longValueExact();
        } else {
            throw new IllegalArgumentException("cannot convert to an integer");
        }
        return checkRange(v, min, max, "value");
    }

    private static long checkRange(final long v, final long min, final long max, final String what) {
        if (v < min || v > max) {
            throw new IllegalArgumentException(what + " " + v + " is outside " + min + ".." + max);
        }
        return v;
    }

    private static BigInteger toBigInteger(final Object value) {
        if (value == null) {
            return BigInteger.ZERO;
        }
        if (value instanceof BigInteger b) {
            return b;
        }
        if (value instanceof BigDecimal d) {
            return d.toBigIntegerExact();
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        if (value instanceof Number n) {
            return new BigDecimal(n.toString()).toBigIntegerExact();
        }
        if (value instanceof Boolean b) {
            return b ? BigInteger.ONE : BigInteger.ZERO;
        }
        if (value instanceof CharSequence s) {
            return new BigDecimal(s.toString().trim()).toBigIntegerExact();
        }
        throw new IllegalArgumentException("cannot convert to an integer");
    }

    private static double toDouble(final Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof CharSequence s) {
            return Double.parseDouble(s.toString().trim());
        }
        if (value instanceof Boolean b) {
            return b ? 1 : 0;
        }
        throw new IllegalArgumentException("cannot convert to a floating point number");
    }

    private static BigDecimal toBigDecimal(final Object value) {
        if (value == null) {
            return BigDecimal.ZERO;
        }
        if (value instanceof BigDecimal d) {
            return d;
        }
        if (value instanceof BigInteger b) {
            return new BigDecimal(b);
        }
        if (value instanceof Double || value instanceof Float) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof Number n) {
            return BigDecimal.valueOf(n.longValue());
        }
        if (value instanceof CharSequence s) {
            return new BigDecimal(s.toString().trim());
        }
        throw new IllegalArgumentException("cannot convert to a decimal");
    }

    private static byte[] toBytes(final Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof Byte[] boxed) {
            final byte[] bytes = new byte[boxed.length];
            for (int i = 0; i < boxed.length; i++) {
                bytes[i] = boxed[i];
            }
            return bytes;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean toBoolean(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.doubleValue() != 0;
        }
        if (value instanceof CharSequence s) {
            final String text = s.toString().trim().toLowerCase();
            return switch (text) {
                case "true", "1", "yes", "y", "t" -> true;
                case "false", "0", "no", "n", "f", "" -> false;
                default -> throw new IllegalArgumentException("'" + text + "' is not a boolean");
            };
        }
        throw new IllegalArgumentException("cannot convert to a boolean");
    }

    private static UUID toUuid(final Object value) {
        if (value == null) {
            return new UUID(0, 0);
        }
        if (value instanceof UUID u) {
            return u;
        }
        if (value instanceof CharSequence s) {
            return UUID.fromString(s.toString().trim());
        }
        throw new IllegalArgumentException("cannot convert to a UUID");
    }

    private static long toEpochDay(final Object value, final ZoneId zone) {
        if (value == null) {
            return 0;
        }
        if (value instanceof java.sql.Date d) {
            return d.toLocalDate().toEpochDay();
        }
        if (value instanceof LocalDate d) {
            return d.toEpochDay();
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof CharSequence s) {
            final String text = s.toString().trim();
            if (text.length() > 10) {
                return toInstant(text, zone).atZone(ZoneOffset.UTC).toLocalDate().toEpochDay();
            }
            return LocalDate.parse(text).toEpochDay();
        }
        return toInstant(value, zone).atZone(ZoneOffset.UTC).toLocalDate().toEpochDay();
    }

    private static Instant toInstant(final Object value, final ZoneId zone) {
        if (value == null) {
            return Instant.EPOCH;
        }
        if (value instanceof java.sql.Date d) {
            return d.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        if (value instanceof java.util.Date d) {
            return d.toInstant();
        }
        if (value instanceof Instant i) {
            return i;
        }
        if (value instanceof OffsetDateTime t) {
            return t.toInstant();
        }
        if (value instanceof ZonedDateTime t) {
            return t.toInstant();
        }
        if (value instanceof LocalDateTime t) {
            return t.atZone(zone).toInstant();
        }
        if (value instanceof LocalDate d) {
            return d.atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        if (value instanceof Number n) {
            // epoch seconds, fraction allowed
            final BigDecimal seconds = toBigDecimal(n);
            final long whole = seconds.setScale(0, RoundingMode.FLOOR).longValueExact();
            final long nanos = seconds.subtract(BigDecimal.valueOf(whole)).movePointRight(9).longValue();
            return Instant.ofEpochSecond(whole, nanos);
        }
        if (value instanceof CharSequence s) {
            return toInstant(s.toString().trim(), zone);
        }
        throw new IllegalArgumentException("cannot convert to a timestamp");
    }

    private static Instant toInstant(final String text, final ZoneId zone) {
        final String iso = text.length() > 10 && text.charAt(10) == ' ' ? text.substring(0, 10) + 'T' + text.substring(11) : text;
        try {
            return OffsetDateTime.parse(iso).toInstant();
        } catch (final DateTimeParseException e) {
            try {
                return ZonedDateTime.parse(iso).toInstant();
            } catch (final DateTimeParseException e2) {
                return LocalDateTime.parse(iso).atZone(zone).toInstant();
            }
        }
    }

    private static long toDateTime64(final Instant instant, final int scale) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), POW10[scale]), instant.getNano() / POW10[9 - scale]);
    }

    private static int toEnumValue(final Object value, final ClickHouseEnum constants, final int min, final int max) {
        final int v;
        if (value == null) {
            v = constants.getValues()[0];
        } else if (value instanceof Number n) {
            v = constants.validate(n.intValue());
        } else if (value instanceof CharSequence s) {
            final String text = s.toString();
            if (!text.isEmpty() && (Character.isDigit(text.charAt(0)) || text.charAt(0) == '-') && text.chars().skip(1).allMatch(Character::isDigit)) {
                v = constants.validate(Integer.parseInt(text));
            } else {
                v = constants.value(text);
            }
        } else if (value instanceof Enum<?> e) {
            v = constants.value(e.name());
        } else {
            throw new IllegalArgumentException("cannot convert to an enum value");
        }
        return (int) checkRange(v, min, max, "enum value");
    }

    // ---- writers -------------------------------------------------------------------------------

    private static void writeDecimal(final OutputStream out, final ClickHouseColumn column, final Object value) throws IOException {
        final int precision = column.getPrecision();
        final BigInteger unscaled = toBigDecimal(value).setScale(column.getScale(), RoundingMode.HALF_UP).unscaledValue();
        if (unscaled.abs().toString().length() > precision) {
            throw new IllegalArgumentException("value " + value + " does not fit in Decimal(" + precision + ", " + column.getScale() + ")");
        }
        final int width = precision <= 9 ? 4 : precision <= 18 ? 8 : precision <= 38 ? 16 : 32;
        writeBigIntegerLE(out, unscaled, width, null, null);
    }

    private static void writeString(final OutputStream out, final byte[] bytes) throws IOException {
        writeVarInt(out, bytes.length);
        out.write(bytes);
    }

    private static void writeFixedString(final OutputStream out, final byte[] bytes, final int length) throws IOException {
        if (bytes.length > length) {
            throw new IllegalArgumentException("value of " + bytes.length + " bytes is longer than FixedString(" + length + ")");
        }
        out.write(bytes);
        for (int i = bytes.length; i < length; i++) {
            out.write(0);
        }
    }

    private static void writeUuid(final OutputStream out, final UUID uuid) throws IOException {
        writeLongLE(out, uuid.getMostSignificantBits());
        writeLongLE(out, uuid.getLeastSignificantBits());
    }

    private static void writeArray(final OutputStream out, final ClickHouseColumn element, final Object value, final ZoneId zone) throws IOException {
        if (value == null) {
            writeVarInt(out, 0);
            return;
        }
        if (value instanceof Collection<?> c) {
            writeVarInt(out, c.size());
            for (final Object item : c) {
                writeValue(out, element, item, zone);
            }
            return;
        }
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("cannot convert to an array");
        }
        final int length = Array.getLength(value);
        writeVarInt(out, length);
        for (int i = 0; i < length; i++) {
            writeValue(out, element, Array.get(value, i), zone);
        }
    }

    static void writeVarInt(final OutputStream out, long value) throws IOException {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static void writeShortLE(final OutputStream out, final int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void writeIntLE(final OutputStream out, final int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }

    private static void writeLongLE(final OutputStream out, final long v) throws IOException {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** Two's complement, little-endian, exactly {@code width} bytes. */
    private static void writeBigIntegerLE(final OutputStream out, final BigInteger v, final int width, final BigInteger min, final BigInteger max) throws IOException {
        if (min != null && (v.compareTo(min) < 0 || v.compareTo(max) > 0)) {
            throw new IllegalArgumentException("value " + v + " is outside " + min + ".." + max);
        }
        final byte[] big = v.toByteArray(); // big-endian, two's complement, minimal length
        if (big.length > width && !(big.length == width + 1 && big[0] == 0)) {
            throw new IllegalArgumentException("value " + v + " does not fit in " + width + " bytes");
        }
        final int fill = v.signum() < 0 ? 0xFF : 0;
        for (int i = 0; i < width; i++) {
            final int index = big.length - 1 - i;
            out.write(index >= 0 ? big[index] & 0xFF : fill);
        }
    }
}
