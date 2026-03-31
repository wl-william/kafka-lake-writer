package com.lakewriter.worker.writer;

import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.storage.StorageAdapter;

/**
 * Creates the correct FormatWriter based on TopicSinkConfig.sinkFormat.
 */
public class FileWriterFactory {

    private final StorageAdapter storage;

    public FileWriterFactory(StorageAdapter storage) {
        this.storage = storage;
    }

    public FormatWriter create(TopicSinkConfig config) {
        if ("CSV".equalsIgnoreCase(config.getSinkFormat())) {
            return new CsvFileWriter(storage);
        }
        return new ParquetFileWriter(storage);
    }
}
