package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.data.ClickHouseColumn;
import io.github.danmorcov88.stavilar.processors.TableColumns.TableColumn;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compares the encoder's bytes with fixtures produced by clickhouse-local
 * (see src/test/resources/rowbinary/generate.sh).
 */
class RowBinaryEncoderTest {

    private static final ZoneId BUCHAREST = ZoneId.of("Europe/Bucharest");
    private static final Instant TS = Instant.parse("2024-03-15T11:45:10Z");

    /** Java input per fixture name. Several fixtures get more than one input to cover the accepted value kinds. */
    private static final Map<String, List<Object>> INPUTS = new HashMap<>();

    static {
        INPUTS.put("int8_neg", List.of((byte) -5, -5, "-5", -5L, new BigDecimal("-5"), -5.0));
        INPUTS.put("int8_min", List.of(-128));
        INPUTS.put("uint8_max", List.of(255, "255"));
        INPUTS.put("int16", List.of(-300, (short) -300));
        INPUTS.put("uint16", List.of(65535));
        INPUTS.put("int32", List.of(-123456789));
        INPUTS.put("uint32", List.of(4294967295L, "4294967295"));
        INPUTS.put("int64", List.of(-9_000_000_000L, "-9000000000"));
        INPUTS.put("uint64_max", List.of(new BigInteger("18446744073709551615"), "18446744073709551615", new BigDecimal("18446744073709551615")));
        INPUTS.put("int128_min", List.of(new BigInteger("-170141183460469231731687303715884105728")));
        INPUTS.put("uint128", List.of(new BigInteger("340282366920938463463374607431768211455")));
        INPUTS.put("int256_neg", List.of(new BigInteger("-57896044618658097711785492504343953926634992332820282019728792003956564819968")));
        INPUTS.put("uint256_max", List.of(new BigInteger("115792089237316195423570985008687907853269984665640564039457584007913129639935")));
        INPUTS.put("float32", List.of(1.5f, 1.5, "1.5", 1.5d));
        INPUTS.put("float64", List.of(-2.25, -2.25f, "-2.25"));
        INPUTS.put("float64_nan", List.of(Double.NaN));
        INPUTS.put("dec32", List.of(new BigDecimal("12345.67"), "12345.67", 12345.67));
        INPUTS.put("dec64_neg", List.of(new BigDecimal("-1.2345"), "-1.2345"));
        INPUTS.put("dec128", List.of(new BigDecimal("123456789012345678.1234567891")));
        INPUTS.put("dec256", List.of(new BigDecimal("-99999999999999999999999999999999999999999999999999999999999999999999999.99999")));
        INPUTS.put("string", List.of("Stăvilar ✓", "Stăvilar ✓".getBytes(StandardCharsets.UTF_8)));
        INPUTS.put("string_empty", List.of(""));
        INPUTS.put("string_long", List.of("x".repeat(200)));
        INPUTS.put("fixed_string", List.of("ab", "ab".getBytes(StandardCharsets.UTF_8)));
        INPUTS.put("fixed_string_full", List.of("abc"));
        INPUTS.put("bool_true", List.of(true, 1, "true", "1"));
        INPUTS.put("bool_false", List.of(false, 0, "false", "0"));
        INPUTS.put("uuid", List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), "123e4567-e89b-12d3-a456-426614174000"));
        INPUTS.put("date", List.of(LocalDate.of(2024, 3, 15), java.sql.Date.valueOf("2024-03-15"), "2024-03-15", 19797,
                Instant.parse("2024-03-15T23:59:59Z"), "2024-03-15T23:59:59Z"));
        INPUTS.put("date_epoch", List.of(LocalDate.of(1970, 1, 1), 0));
        INPUTS.put("date32_before_epoch", List.of(LocalDate.of(1950, 1, 2), java.sql.Date.valueOf("1950-01-02")));
        INPUTS.put("datetime", List.of(TS, Timestamp.from(TS), java.util.Date.from(TS), OffsetDateTime.ofInstant(TS, ZoneOffset.ofHours(3)),
                TS.atZone(BUCHAREST), LocalDateTime.of(2024, 3, 15, 13, 45, 10), "2024-03-15T11:45:10Z", "2024-03-15T14:45:10+03:00",
                "2024-03-15 13:45:10", "2024-03-15T13:45:10", 1710503110L, 1710503110.0));
        INPUTS.put("datetime64_ms", List.of(Instant.parse("2024-03-15T11:45:10.123Z"), Timestamp.from(Instant.parse("2024-03-15T11:45:10.123Z")),
                "2024-03-15T11:45:10.123Z", new BigDecimal("1710503110.123"), 1710503110.123));
        INPUTS.put("datetime64_ns", List.of(Instant.parse("2024-03-15T11:45:10.123456789Z"), Timestamp.valueOf(LocalDateTime.of(2024, 3, 15, 13, 45, 10, 123_456_789))));
        INPUTS.put("datetime64_before_epoch", List.of(Instant.parse("1969-12-31T23:59:59.500Z"), new BigDecimal("-0.5")));
        INPUTS.put("enum8", List.of("b", 2, "2"));
        INPUTS.put("enum16_neg", List.of("x", -5, "-5"));
        INPUTS.put("nullable_null", Arrays.asList((Object) null));
        INPUTS.put("nullable_value", List.of(42));
        INPUTS.put("nullable_string_null", Arrays.asList((Object) null));
        INPUTS.put("lowcardinality", List.of("low"));
        INPUTS.put("lowcardinality_nullable", List.of("x"));
        INPUTS.put("array_empty", List.of(new Object[0], List.of(), new int[0]));
        INPUTS.put("array_int", List.of(new Object[]{1, -2, 3}, List.of(1, -2, 3), new int[]{1, -2, 3}, new String[]{"1", "-2", "3"}));
        INPUTS.put("array_nullable", List.of(new Object[]{1, null, 3}, Arrays.asList(1, null, 3)));
        INPUTS.put("array_nested", List.of(new Object[]{new Object[]{"a", "b"}, new Object[0], new Object[]{"c"}}, List.of(List.of("a", "b"), List.of(), List.of("c"))));
        INPUTS.put("array_datetime", List.of(new Object[]{TS}, List.of(Timestamp.from(TS))));
    }

    record Fixture(String name, String type, String expectedHex) {
        @Override
        public String toString() {
            return name + " " + type;
        }
    }

    static Stream<Fixture> fixtures() throws IOException {
        try (InputStream in = RowBinaryEncoderTest.class.getResourceAsStream("/rowbinary/cases.tsv")) {
            final String[] lines = new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n");
            return Arrays.stream(lines).filter(l -> !l.isBlank()).map(line -> {
                final String[] parts = line.split("\t");
                return new Fixture(parts[0], parts[1], readHex(parts[0]));
            });
        }
    }

    private static String readHex(final String name) {
        try (InputStream in = RowBinaryEncoderTest.class.getResourceAsStream("/rowbinary/expected/" + name + ".hex")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void matchesClickHouseLocal(final Fixture fixture) throws IOException {
        final List<Object> inputs = INPUTS.get(fixture.name());
        assertTrue(inputs != null && !inputs.isEmpty(), "no Java input for fixture " + fixture.name());
        final ClickHouseColumn column = ClickHouseColumn.of(fixture.name(), fixture.type());
        for (final Object input : inputs) {
            assertEquals(fixture.expectedHex(), encode(column, input),
                    fixture + " from " + (input == null ? "null" : input.getClass().getSimpleName() + " " + describe(input)));
        }
    }

    @Test
    void everyFixtureHasAnInput() throws IOException {
        final List<String> missing = fixtures().map(Fixture::name).filter(n -> !INPUTS.containsKey(n)).toList();
        assertEquals(List.of(), missing);
    }

    @Test
    void nullBecomesDefaultForNonNullableColumns() throws IOException {
        assertEquals("00000000", encode(ClickHouseColumn.of("c", "Int32"), null));
        assertEquals("00", encode(ClickHouseColumn.of("c", "String"), null));
        assertEquals("000000", encode(ClickHouseColumn.of("c", "FixedString(3)"), null));
        assertEquals("00", encode(ClickHouseColumn.of("c", "Bool"), null));
        assertEquals("00000000000000000000000000000000", encode(ClickHouseColumn.of("c", "UUID"), null));
        assertEquals("0000", encode(ClickHouseColumn.of("c", "Date"), null));
        assertEquals("0000000000000000", encode(ClickHouseColumn.of("c", "DateTime64(3)"), null));
        assertEquals("01", encode(ClickHouseColumn.of("c", "Enum8('a' = 1, 'b' = 2)"), null));
        assertEquals("00", encode(ClickHouseColumn.of("c", "Array(Int32)"), null));
        assertEquals("00000000", encode(ClickHouseColumn.of("c", "Decimal(9, 2)"), null));
    }

    @Test
    void rejectsValuesThatDoNotFit() {
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Int8"), 128));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "UInt8"), -1));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "UInt64"), -1));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "UInt64"), new BigInteger("18446744073709551616")));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Int32"), 1.5));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Int32"), "abc"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Decimal(9, 2)"), new BigDecimal("12345678.99")));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "FixedString(3)"), "abcd"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Enum8('a' = 1)"), "zzz"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Enum8('a' = 1)"), 7));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Date"), LocalDate.of(1960, 1, 1)));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "DateTime"), Instant.parse("1960-01-01T00:00:00Z")));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "UUID"), "not-a-uuid"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Bool"), "maybe"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Array(Int32)"), "not an array"));
        assertThrows(IllegalArgumentException.class, () -> encode(ClickHouseColumn.of("c", "Map(String, Int32)"), Map.of()));
    }

    @Test
    void errorMessagesNameTheColumnAndType() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> encode(ClickHouseColumn.of("amount", "Decimal(9, 2)"), "abc"));
        assertTrue(e.getMessage().startsWith("column 'amount' (Decimal(9, 2)): "), e.getMessage());
        assertTrue(e.getMessage().contains("[value of type String]"), e.getMessage());
    }

    @Test
    void decimalsAreRoundedHalfUpToScale() throws IOException {
        // 12345.675 rounds to 12345.68 -> 1234568 = 0x0012D688
        assertEquals("88d61200", encode(ClickHouseColumn.of("c", "Decimal(9, 2)"), new BigDecimal("12345.675")));
    }

    @Test
    void writesWholeRecordsInColumnOrder() throws IOException {
        final List<TableColumn> table = ClickHouseColumn.parse("id UInt32, name String, flag Nullable(Bool)").stream()
                .map(c -> new TableColumn(c, true)).toList();
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", "ab");
        values.put("id", 7);
        final MapRecord record = new MapRecord(new SimpleRecordSchema(List.of(
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("id", RecordFieldType.INT.getDataType()))), values);
        final List<ClickHouseColumn> columns = ColumnMapping.forRecordSchema(record.getSchema(), table, "t");
        assertEquals(List.of("id", "name"), columns.stream().map(ClickHouseColumn::getColumnName).toList());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (RowBinaryEncoder encoder = new RowBinaryEncoder(out, columns, BUCHAREST)) {
            encoder.write(record);
            encoder.write(record);
            assertEquals(2, encoder.getRowCount());
        }
        assertEquals("07000000026162" + "07000000026162", HexFormat.of().formatHex(out.toByteArray()));
    }

    @Test
    void mappingRejectsComputedColumnsAndUnknownFields() {
        final List<TableColumn> table = List.of(
                new TableColumn(ClickHouseColumn.of("id", "UInt32"), true),
                new TableColumn(ClickHouseColumn.of("doubled", "UInt64"), false),
                new TableColumn(ClickHouseColumn.of("tags", "Map(String, String)"), true));
        final SimpleRecordSchema idAndDoubled = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("doubled", RecordFieldType.LONG.getDataType())));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ColumnMapping.forRecordSchema(idAndDoubled, table, "t"))
                .getMessage().contains("ALIAS or MATERIALIZED"));

        final SimpleRecordSchema idAndNope = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("nope", RecordFieldType.STRING.getDataType())));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ColumnMapping.forRecordSchema(idAndNope, table, "t"))
                .getMessage().contains("[nope]"));

        final SimpleRecordSchema idAndTags = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("tags", RecordFieldType.MAP.getDataType())));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ColumnMapping.forRecordSchema(idAndTags, table, "t"))
                .getMessage().contains("Insert Format = JSONEachRow"));

        assertEquals("t", TableColumns.unquote("`t`"));
        assertEquals("t", TableColumns.unquote(" t "));
    }

    @Test
    void fastIsoParserAgreesWithJavaTime() {
        for (final String text : List.of("2024-03-15T11:45:10Z", "2024-03-15 11:45:10Z", "2024-03-15T11:45:10.123Z", "2024-03-15T11:45:10.123456789Z",
                "2024-03-15T14:45:10+03:00", "2024-03-15T06:45:10-05:00", "1969-12-31T23:59:59.5Z", "2000-02-29T00:00:00Z", "1900-03-01T00:00:00Z")) {
            final Instant expected = OffsetDateTime.parse(text.replace(' ', 'T')).toInstant();
            assertEquals(expected, RowBinaryEncoder.parseIsoInstant(text, BUCHAREST), text);
        }
        assertEquals(LocalDateTime.of(2024, 3, 15, 13, 45, 10).atZone(BUCHAREST).toInstant(),
                RowBinaryEncoder.parseIsoInstant("2024-03-15 13:45:10", BUCHAREST));
        for (final String bad : List.of("2024-03-15", "2024-13-15T11:45:10Z", "2024-03-15T11:45:10.Z", "2024-03-15T11:45:10+0300", "15/03/2024 11:45:10")) {
            assertNull(RowBinaryEncoder.parseIsoInstant(bad, BUCHAREST), bad);
        }
    }

    @Test
    void varIntEncoding() throws IOException {
        for (final long[] c : new long[][]{{0, 0}, {127, 0x7F}, {128, 0x8001}, {200, 0xC801}, {16384, 0x808001}}) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            RowBinaryEncoder.writeVarInt(out, c[0]);
            assertEquals(new BigInteger(Long.toString(c[1])).toString(16), new BigInteger(1, out.toByteArray()).toString(16), "varint " + c[0]);
        }
    }

    private static String encode(final ClickHouseColumn column, final Object value) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryEncoder.writeValue(out, column, value, BUCHAREST);
        return HexFormat.of().formatHex(out.toByteArray());
    }

    private static String describe(final Object input) {
        if (input instanceof Object[] a) {
            return Arrays.deepToString(a);
        }
        if (input instanceof byte[] b) {
            return HexFormat.of().formatHex(b);
        }
        if (input instanceof int[] a) {
            return Arrays.toString(a);
        }
        return String.valueOf(input);
    }
}
