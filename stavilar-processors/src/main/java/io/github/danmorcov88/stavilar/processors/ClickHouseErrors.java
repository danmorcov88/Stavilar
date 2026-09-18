package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.TransportException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Classifies client-v2 failures for routing: retry or failure.
 * <p>
 * client-v2 hierarchy: {@code ClickHouseException} is the base; {@code ServerException} carries the
 * server's error code and its own retryable flag; {@code ConnectionInitiationException},
 * {@code TransportException} and {@code DataTransferException} are network problems;
 * {@code ClientException} covers client-side problems such as bad options.
 */
final class ClickHouseErrors {

    private ClickHouseErrors() {
    }

    /** Unwraps the future wrappers client-v2 may put around the real cause. */
    static Throwable unwrap(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    /**
     * @return true for network problems and server errors ClickHouse marks as retryable
     * (too many parts, memory limit, timeouts); false for data, schema and client configuration errors.
     * The whole cause chain is inspected: client-v2 wraps a transport failure in a generic
     * {@code ClientException("Failed to get query response")} on some paths.
     */
    static boolean isRetryable(final Throwable t) {
        for (Throwable cause = unwrap(t); cause != null; cause = cause.getCause()) {
            if (cause instanceof ServerException server) {
                return server.isRetryable();
            }
            if (cause instanceof ConnectionInitiationException || cause instanceof TransportException || cause instanceof DataTransferException) {
                return true;
            }
        }
        return false;
    }

    /** @return true when ClickHouse itself answered with an error (as opposed to a client or network problem) */
    static boolean isServerError(final Throwable t) {
        for (Throwable cause = unwrap(t); cause != null; cause = cause.getCause()) {
            if (cause instanceof ServerException) {
                return true;
            }
        }
        return false;
    }

    /** The message of the unwrapped exception, plus the root cause's message when that adds something. */
    static String message(final Throwable t) {
        final Throwable cause = unwrap(t);
        final String message = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root != cause && root.getMessage() != null && !message.contains(root.getMessage())) {
            return message + ": " + root.getMessage();
        }
        return message;
    }
}
