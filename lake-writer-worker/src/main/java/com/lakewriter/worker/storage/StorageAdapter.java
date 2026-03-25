package com.lakewriter.worker.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Abstracts HDFS / OSS / S3 storage operations.
 * Workers interact only with this interface — swapping implementations doesn't change consumer logic.
 */
public interface StorageAdapter extends AutoCloseable {

    /** Open a writable output stream at path (creates parent dirs automatically) */
    OutputStream create(String path) throws IOException;

    /** Atomic rename — HDFS rename is atomic within a filesystem */
    boolean rename(String srcPath, String dstPath) throws IOException;

    /** Check whether a file or directory exists */
    boolean exists(String path) throws IOException;

    /** Delete a single file (non-recursive) */
    boolean delete(String path) throws IOException;

    /** List file paths (not directories) directly under dirPath */
    List<String> listFiles(String dirPath) throws IOException;

    /** Create directory and all parents */
    boolean mkdirs(String path) throws IOException;

    @Override
    void close() throws IOException;
}
