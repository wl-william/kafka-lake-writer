package com.lakewriter.worker.writer;

import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;

import java.io.IOException;
import java.util.List;

/**
 * Writes rows to a single file (Parquet or CSV).
 * Lifecycle: open() → writeRow()/writeRows() → close()
 */
public interface FormatWriter extends AutoCloseable {

    void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException;

    void writeRow(Object[] row) throws IOException;

    default void writeRows(List<Object[]> rows) throws IOException {
        for (Object[] row : rows) {
            writeRow(row);
        }
    }

    @Override
    void close() throws IOException;

    /** Estimated bytes written so far */
    long getWrittenBytes();

    long getWrittenRows();
}
