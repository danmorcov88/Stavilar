package io.github.danmorcov88.stavilar.processors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the {@code X-ClickHouse-Summary} response header, a flat JSON object such as
 * {@code {"read_rows":"2","written_rows":"2","elapsed_ns":"60800784",...}}, into
 * FlowFile attributes named {@code clickhouse.summary.<key>}.
 */
final class ClickHouseSummary {

    static final String HEADER = "X-ClickHouse-Summary";
    static final String ATTRIBUTE_PREFIX = "clickhouse.summary.";

    private static final JsonFactory FACTORY = new JsonFactory();

    private ClickHouseSummary() {
    }

    static Map<String, String> toAttributes(final Map<String, String> responseHeaders) {
        if (responseHeaders == null) {
            return Map.of();
        }
        for (final Map.Entry<String, String> header : responseHeaders.entrySet()) {
            if (HEADER.equalsIgnoreCase(header.getKey())) {
                return parse(header.getValue());
            }
        }
        return Map.of();
    }

    /** @return the summary keys as attributes, in header order; empty when the text is not a flat JSON object */
    static Map<String, String> parse(final String json) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return attributes;
        }
        try (JsonParser parser = FACTORY.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return Map.of();
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                final String key = parser.currentName();
                final JsonToken value = parser.nextToken();
                if (value == JsonToken.START_OBJECT || value == JsonToken.START_ARRAY) {
                    parser.skipChildren();
                } else {
                    attributes.put(ATTRIBUTE_PREFIX + key, parser.getText());
                }
            }
        } catch (final IOException e) {
            return Map.of();
        }
        return attributes;
    }
}
