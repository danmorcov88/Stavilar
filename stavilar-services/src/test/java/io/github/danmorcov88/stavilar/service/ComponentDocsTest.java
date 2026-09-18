package io.github.danmorcov88.stavilar.service;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentDocsTest {

    @Test
    void registeredServiceHasAdditionalDetails() throws IOException {
        final String service;
        try (InputStream in = ComponentDocsTest.class.getResourceAsStream("/META-INF/services/org.apache.nifi.controller.ControllerService")) {
            assertNotNull(in);
            service = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        try (InputStream in = ComponentDocsTest.class.getResourceAsStream("/docs/" + service + "/additionalDetails.md")) {
            assertNotNull(in, "missing docs for " + service);
            assertTrue(new String(in.readAllBytes(), StandardCharsets.UTF_8).startsWith("# StandardClickHouseConnectionService"));
        }
    }
}
