package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.TransportException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Decides whether a failed insert is worth retrying.
 * <p>
 * client-v2 hierarchy: {@code ClickHouseException} is the base; {@code ServerException} carries the
 * server's error code and its own retryable flag; {@code ConnectionInitiationException},
 * {@code TransportException} and {@code DataTransferException} are network problems;
 * {@code ClientException} covers client-side problems such as bad options.
 */
final class InsertErrors {

    private InsertErrors() {
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
     * (too many parts, memory limit, timeouts); false for data, schema and client configuration errors
     */
    static boolean isRetryable(final Throwable t) {
        final Throwable cause = unwrap(t);
        if (cause instanceof ServerException server) {
            return server.isRetryable();
        }
        return cause instanceof ConnectionInitiationException
                || cause instanceof TransportException
                || cause instanceof DataTransferException;
    }

    static String message(final Throwable t) {
        final Throwable cause = unwrap(t);
        final String message = cause.getMessage();
        return message == null ? cause.getClass().getSimpleName() : message;
    }
}
