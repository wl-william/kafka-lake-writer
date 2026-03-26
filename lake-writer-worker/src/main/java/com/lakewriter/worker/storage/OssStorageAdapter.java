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

    private static FileSystem buildOssFileSystem(String endpoint,
                                                  String accessKeyId,
                                                  String accessKeySecret,
                                                  String bucket) throws IOException {
        Configuration conf = new Configuration();
        // Register Aliyun OSS FileSystem for oss:// scheme
        conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        conf.set("fs.oss.endpoint", endpoint);
        conf.set("fs.oss.accessKeyId", accessKeyId);
        conf.set("fs.oss.accessKeySecret", accessKeySecret);
        // Disable Hadoop default caching so credentials are applied immediately
        conf.set("fs.oss.impl.disable.cache", "false");
        URI ossUri = URI.create("oss://" + bucket + "/");
        return FileSystem.get(ossUri, conf);
    }
}
