package com.lakewriter.worker.storage;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * OSS implementation of StorageAdapter via Hadoop FileSystem SPI (Method A).
 *
 * Configures Hadoop to use AliyunOSSFileSystem for oss:// paths, which allows
 * Parquet writers and all existing storage operations to work transparently with OSS
 * without any changes to upstream code (FlushExecutor, CrashRecoveryManager, etc.).
 *
 * Required dependency: hadoop-aliyun (fs.oss.impl = AliyunOSSFileSystem).
 * Registered via Hadoop SPI — no extra code paths needed in callers.
 *
 * IMPORTANT — OSS rename is NOT atomic (unlike HDFS):
 * AliyunOSSFileSystem.rename() is a server-side copy + delete.
 * If the process crashes between copy completion and source deletion, both paths exist.
 * The overridden rename() below handles this idempotently: if the destination already
 * exists the copy already succeeded, so we skip the copy and just remove the source.
 * This makes Phase 3 of the 5-phase commit crash-safe for OSS.
 */
@Slf4j
public class OssStorageAdapter extends HadoopStorageAdapter {

    /**
     * @param endpoint        OSS endpoint, e.g. oss-cn-hangzhou.aliyuncs.com
     * @param accessKeyId     Aliyun RAM access key ID
     * @param accessKeySecret Aliyun RAM access key secret
     * @param bucket          OSS bucket name (paths will be oss://bucket/...)
     */
    public OssStorageAdapter(String endpoint,
                             String accessKeyId,
                             String accessKeySecret,
                             String bucket) throws IOException {
        super(buildOssFileSystem(endpoint, accessKeyId, accessKeySecret, bucket));
        log.info("OssStorageAdapter initialized, endpoint={} bucket={}", endpoint, bucket);
    }

    /**
     * Idempotent rename for OSS crash recovery.
     *
     * If destination already exists (crash happened after copy but before source delete),
     * the data is already fully written — skip the re-copy and just remove the source.
     * Without this override, a second rename() call could fail on some OSS Hadoop versions
     * when the destination already exists, causing C-5 crash recovery to incorrectly roll back.
     */
    @Override
    public boolean rename(String srcPath, String dstPath) throws IOException {
        if (exists(dstPath)) {
            log.info("[OSS] rename: target already exists (prior crash?), deleting source only: {}", srcPath);
            delete(srcPath);
            return true;
        }
        return super.rename(srcPath, dstPath);
    }

    private static FileSystem buildOssFileSystem(String endpoint,
                                                  String accessKeyId,
                                                  String accessKeySecret,
                                                  String bucket) throws IOException {
        Configuration conf = new Configuration();
        URI ossUri = URI.create("oss://" + bucket + "/");
        // Register Aliyun OSS FileSystem for oss:// scheme
        conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        conf.set("fs.oss.endpoint", endpoint);
        conf.set("fs.oss.accessKeyId", accessKeyId);
        conf.set("fs.oss.accessKeySecret", accessKeySecret);
        conf.set("fs.defaultFS", ossUri.toString());
        // Disable Hadoop default caching so credentials are applied immediately
        conf.set("fs.oss.impl.disable.cache", "false");
        return FileSystem.get(ossUri, conf);
    }
}
