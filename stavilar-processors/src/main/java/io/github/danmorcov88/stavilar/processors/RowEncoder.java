package io.github.danmorcov88.stavilar.processors;

import org.apache.nifi.serialization.record.Record;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes records in one ClickHouse input format.
 */
interface RowEncoder extends Closeable {

    void write(Record record) throws IOException;

    long getRowCount();
}
