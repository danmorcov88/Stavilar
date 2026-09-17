package io.github.danmorcov88.stavilar.service;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses and validates a comma-separated list of {@code host:port} entries.
 */
final class EndpointsValidator implements Validator {

    static final EndpointsValidator INSTANCE = new EndpointsValidator();

    record HostPort(String host, int port) {
    }

    private EndpointsValidator() {
    }

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder result = new ValidationResult.Builder().subject(subject).input(input);
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return result.valid(true).explanation("Expression Language present").build();
        }
        try {
            parse(input);
            return result.valid(true).build();
        } catch (final IllegalArgumentException e) {
            return result.valid(false).explanation(e.getMessage()).build();
        }
    }

    /**
     * @throws IllegalArgumentException when the value is empty or an entry is not {@code host:port}
     */
    static List<HostPort> parse(final String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("at least one host:port entry is required");
        }
        final List<HostPort> endpoints = new ArrayList<>();
        for (final String raw : value.split(",")) {
            final String entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }
            final int colon = entry.lastIndexOf(':');
            if (colon <= 0 || colon == entry.length() - 1) {
                throw new IllegalArgumentException("'" + entry + "' is not in host:port form");
            }
            final String host = entry.substring(0, colon);
            final int port;
            try {
                port = Integer.parseInt(entry.substring(colon + 1));
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("'" + entry + "' has a non-numeric port");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("'" + entry + "' has a port outside 1-65535");
            }
            endpoints.add(new HostPort(host, port));
        }
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one host:port entry is required");
        }
        return endpoints;
    }
}
