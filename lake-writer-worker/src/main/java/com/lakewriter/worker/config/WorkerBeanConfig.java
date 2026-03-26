package com.lakewriter.worker.config;

import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.CheckpointManager;
import com.lakewriter.worker.checkpoint.CrashRecoveryManager;
import com.lakewriter.worker.checkpoint.IdempotentWriteChecker;
import com.lakewriter.worker.consumer.KafkaConsumerPool;
import com.lakewriter.worker.heartbeat.HeartbeatReporter;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.storage.HadoopStorageAdapter;
import com.lakewriter.worker.storage.OssStorageAdapter;
import com.lakewriter.worker.storage.StorageAdapter;
import com.lakewriter.worker.writer.FlushExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Spring @Configuration for all Worker beans.
 *
 * All non-@Component worker objects (StorageAdapter, FlushExecutor, TopicMatcher,
 * WriteBufferManager, KafkaConsumerPool, etc.) are created and wired here.
 * ConfigPoller, HeartbeatReporter, and GracefulShutdownHook are @Component
 * and consume these beans via constructor injection.
 */
@Slf4j
@org.springframework.context.annotation.Configuration
public class WorkerBeanConfig {

    // ===== Kafka =====
    @Value("${lake-writer.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}")
    private String bootstrapServers;

    @Value("${lake-writer.kafka.group-id:kafka-lake-writer}")
    private String groupId;

    @Value("${lake-writer.kafka.consumer-count:4}")
    private int consumerCount;

    @Value("${lake-writer.kafka.max-poll-records:5000}")
    private int maxPollRecords;

    @Value("${lake-writer.kafka.fetch-min-bytes:1048576}")
    private int fetchMinBytes;

    @Value("${lake-writer.kafka.fetch-max-wait-ms:500}")
    private int fetchMaxWaitMs;

    @Value("${lake-writer.kafka.session-timeout-ms:30000}")
    private int sessionTimeoutMs;

    @Value("${lake-writer.kafka.heartbeat-interval-ms:10000}")
    private int heartbeatIntervalMs;

    @Value("${lake-writer.kafka.max-poll-interval-ms:300000}")
    private int maxPollIntervalMs;

    @Value("${lake-writer.kafka.auto-offset-reset:latest}")
    private String autoOffsetReset;

    // ===== Storage =====
    @Value("${lake-writer.storage.type:hdfs}")
    private String storageType;

    @Value("${lake-writer.storage.hdfs.default-fs:hdfs://namenode:8020}")
    private String hdfsDefaultFs;

    @Value("${lake-writer.storage.oss.endpoint:}")
    private String ossEndpoint;

    @Value("${lake-writer.storage.oss.access-key-id:}")
    private String ossAccessKeyId;

    @Value("${lake-writer.storage.oss.access-key-secret:}")
    private String ossAccessKeySecret;

    @Value("${lake-writer.storage.oss.bucket:}")
    private String ossBucket;

    // ===== Buffer =====
    @Value("${lake-writer.buffer.max-total-bytes:3221225472}")
    private long maxTotalBytes;

    // ===== Checkpoint =====
    @Value("${lake-writer.checkpoint.dir:./checkpoint}")
    private String checkpointDir;

    // ===== Node identity =====
    @Value("${lake-writer.node.id:}")
    private String configuredNodeId;

    @Bean
    public NodeIdentity nodeIdentity() {
        return new NodeIdentity(configuredNodeId);
    }

    @Bean
    public StorageAdapter storageAdapter() throws IOException {
        if ("oss".equalsIgnoreCase(storageType)) {
            log.info("StorageAdapter: using OSS (endpoint={}, bucket={})", ossEndpoint, ossBucket);
            return new OssStorageAdapter(ossEndpoint, ossAccessKeyId, ossAccessKeySecret, ossBucket);
        }
        log.info("StorageAdapter: using HDFS (defaultFS={})", hdfsDefaultFs);
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("dfs.replication", "3");
        hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        return new HadoopStorageAdapter(hadoopConf, hdfsDefaultFs);
    }

    @Bean
    public CheckpointManager checkpointManager() {
        return new CheckpointManager(checkpointDir);
    }

    @Bean
    public CrashRecoveryManager crashRecoveryManager(CheckpointManager checkpointManager,
                                                      StorageAdapter storageAdapter) {
        return new CrashRecoveryManager(checkpointManager, storageAdapter);
    }

    @Bean
    public IdempotentWriteChecker idempotentWriteChecker(StorageAdapter storageAdapter) {
        return new IdempotentWriteChecker(storageAdapter);
    }

    @Bean
    public FlushExecutor flushExecutor(StorageAdapter storageAdapter,
                                        CheckpointManager checkpointManager,
                                        IdempotentWriteChecker idempotentWriteChecker) {
        return new FlushExecutor(storageAdapter, checkpointManager, idempotentWriteChecker);
    }

    @Bean
    public TopicMatcher topicMatcher() {
        return new TopicMatcher();
    }

    @Bean
    public WriteBufferManager writeBufferManager() {
        return new WriteBufferManager(maxTotalBytes);
    }

    @Bean
    public KafkaConsumerPool kafkaConsumerPool(WriteBufferManager writeBufferManager,
                                                FlushExecutor flushExecutor,
                                                TopicMatcher topicMatcher,
                                                NodeIdentity nodeIdentity,
                                                CheckpointManager checkpointManager,
                                                CrashRecoveryManager crashRecoveryManager) {
        Properties kafkaProps = buildKafkaProperties();
        return new KafkaConsumerPool(kafkaProps, consumerCount, writeBufferManager,
            flushExecutor, topicMatcher, nodeIdentity, checkpointManager, crashRecoveryManager);
    }

    /**
     * After all beans are constructed, start the consumer pool.
     * ConfigPoller.init() (@PostConstruct) runs before this, so TopicMatcher is already loaded.
     */
    @PostConstruct
    public void startConsumerPool() {
        // Note: KafkaConsumerPool.start() is called by the application runner below.
        // This method is intentionally empty — startup sequence is managed by WorkerStartupRunner.
    }

    private Properties buildKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // manual commit — At-Least-Once
        return props;
    }
}
