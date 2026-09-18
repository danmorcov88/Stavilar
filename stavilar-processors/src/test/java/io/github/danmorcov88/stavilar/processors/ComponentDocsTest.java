package io.github.danmorcov88.stavilar.processors;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every registered processor ships its NiFi documentation page. A renamed class
 * would otherwise leave an orphan docs folder and an undocumented component.
 */
class ComponentDocsTest {

    @Test
    void everyRegisteredProcessorHasAdditionalDetails() throws IOException {
        final List<String> processors = registered("/META-INF/services/org.apache.nifi.processor.Processor");
        assertEquals(List.of(
                "io.github.danmorcov88.stavilar.processors.PutClickHouseRecord",
                "io.github.danmorcov88.stavilar.processors.QueryClickHouseRecord",
                "io.github.danmorcov88.stavilar.processors.ExecuteClickHouseStatement"), processors);
        for (final String processor : processors) {
            final String path = "/docs/" + processor + "/additionalDetails.md";
            try (InputStream in = ComponentDocsTest.class.getResourceAsStream(path)) {
                assertNotNull(in, "missing " + path);
                final String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                assertTrue(text.startsWith("# " + processor.substring(processor.lastIndexOf('.') + 1)), path + " must start with the component name");
                assertTrue(text.contains("## Relationships") || text.contains("## Routing"), path + " must describe relationships");
            }
        }
    }

    private static List<String> registered(final String resource) throws IOException {
        try (InputStream in = ComponentDocsTest.class.getResourceAsStream(resource)) {
            assertNotNull(in, resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().filter(l -> !l.isBlank()).toList();
        }
    }
}
