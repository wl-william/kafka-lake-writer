package com.lakewriter.worker.storage;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * HDFS implementation via Hadoop FileSystem API.
 * Compatible with CDH6 (hadoop-client 3.0.0-cdh6.3.1).
 */
@Slf4j
public class HadoopStorageAdapter implements StorageAdapter {

    private final FileSystem fs;

    public HadoopStorageAdapter(Configuration conf, String defaultFs) throws IOException {
        conf.set("fs.defaultFS", defaultFs);
        this.fs = FileSystem.get(conf);
        log.info("HadoopStorageAdapter initialized, defaultFS={}", defaultFs);
    }

    /** Constructor with pre-built FileSystem — useful for testing with local FS */
    public HadoopStorageAdapter(FileSystem fs) {
        this.fs = fs;
    }

    public Configuration getConf() {
        return fs.getConf();
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    @Override
    public OutputStream create(String path) throws IOException {
        Path p = new Path(path);
        fs.mkdirs(p.getParent());
        return fs.create(p, true);   // overwrite=true for tmp files
    }

    @Override
    public InputStream read(String path) throws IOException {
        return fs.open(new Path(path));
    }

    @Override
    public boolean rename(String srcPath, String dstPath) throws IOException {
        Path dst = new Path(dstPath);
        fs.mkdirs(dst.getParent());
        boolean result = fs.rename(new Path(srcPath), dst);
        log.debug("rename {} -> {} : {}", srcPath, dstPath, result);
        return result;
    }

    @Override
    public boolean exists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    @Override
    public boolean delete(String path) throws IOException {
        return fs.delete(new Path(path), false);
    }

    @Override
    public List<String> listFiles(String dirPath) throws IOException {
        Path p = new Path(dirPath);
        if (!fs.exists(p)) {
            return Collections.emptyList();
        }
        FileStatus[] statuses = fs.listStatus(p);
        List<String> result = new ArrayList<>(statuses.length);
        for (FileStatus s : statuses) {
            if (!s.isDirectory()) {
                result.add(s.getPath().toString());
            }
        }
        return result;
    }

    @Override
    public boolean mkdirs(String path) throws IOException {
        return fs.mkdirs(new Path(path));
    }

    @Override
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}
