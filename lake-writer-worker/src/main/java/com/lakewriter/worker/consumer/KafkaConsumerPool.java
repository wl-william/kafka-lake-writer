package com.lakewriter.worker.consumer;

import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.CrashRecoveryManager;
import com.lakewriter.worker.checkpoint.IdempotentWriteChecker;
import com.lakewriter.worker.checkpoint.RemoteCheckpointManager;
import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.schema.JsonRecordParser;
import com.lakewriter.worker.storage.StorageHealthChecker;
import com.lakewriter.worker.writer.FlushExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages N KafkaConsumer instances in a Consumer Group.
 * All consumers share the same group.id → Kafka distributes partitions across them.
 *
 * Recovery: at startup, crash recovery runs before any consumer starts polling.
 */
@Slf4j
public class KafkaConsumerPool {

    private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
    private final List<ConsumerWorker> workers = new ArrayList<>();

    /**
     * Thread-safe assignment cache — updated by SafeRebalanceListener on the consumer thread,
     * read by HeartbeatReporter on the scheduling thread.
     * Never call KafkaConsumer.assignment() from outside the consumer thread.
     */
    private final Set<TopicPartition> assignmentCache =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    private ExecutorService consumerPool;
    private ExecutorService flushPool;
    private final Properties kafkaProps;
    private final int consumerCount;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final TopicMatcher topicMatcher;
    private final NodeIdentity nodeIdentity;
    private final RemoteCheckpointManager checkpointMgr;
    private final CrashRecoveryManager recoveryMgr;
    private final IdempotentWriteChecker idempotentChecker;
    private final StorageHealthChecker storageHealthChecker;
    private final JsonRecordParser parser;

    public KafkaConsumerPool(Properties kafkaProps, int consumerCount,
                              WriteBufferManager bufferManager, FlushExecutor flushExecutor,
                              TopicMatcher topicMatcher, NodeIdentity nodeIdentity,
                              RemoteCheckpointManager checkpointMgr, CrashRecoveryManager recoveryMgr,
                              IdempotentWriteChecker idempotentChecker,
                              StorageHealthChecker storageHealthChecker) {
        this.kafkaProps           = kafkaProps;
        this.consumerCount        = consumerCount;
        this.bufferManager        = bufferManager;
        this.flushExecutor        = flushExecutor;
        this.topicMatcher         = topicMatcher;
        this.nodeIdentity         = nodeIdentity;
        this.checkpointMgr        = checkpointMgr;
        this.recoveryMgr          = recoveryMgr;
        this.idempotentChecker    = idempotentChecker;
        this.storageHealthChecker = storageHealthChecker;
        this.parser               = new JsonRecordParser();
    }

    public void start(Set<String> topicsToSubscribe) {
        flushPool = Executors.newFixedThreadPool(4, namedThreadFactory("file-writer"));
        consumerPool = Executors.newFixedThreadPool(consumerCount, namedThreadFactory("kafka-consumer"));

        // Crash recovery is handled entirely by SafeRebalanceListener.onPartitionsAssigned(),
        // which fires after the first poll() when Kafka assigns partitions.
        // At that point consumer.seek() works correctly and checkpoint files are still intact.
        // Do NOT call recoveryMgr.recover() here — it would delete checkpoint files before
        // onPartitionsAssigned() has a chance to read them.
        log.info("[Pool] Starting {} consumers, crash recovery deferred to onPartitionsAssigned", consumerCount);

        for (int i = 0; i < consumerCount; i++) {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
            RecordDispatcher dispatcher = new RecordDispatcher(bufferManager, topicMatcher, parser);

            SafeRebalanceListener rebalanceListener = new SafeRebalanceListener(
                bufferManager, checkpointMgr, recoveryMgr, idempotentChecker,
                flushExecutor, topicMatcher, nodeIdentity, consumer, this);

            consumer.subscribe(topicsToSubscribe, rebalanceListener);

            ConsumerWorker worker = new ConsumerWorker(
                consumer, dispatcher, bufferManager, flushExecutor,
                topicMatcher, nodeIdentity, checkpointMgr, flushPool, storageHealthChecker);

            consumers.add(consumer);
            workers.add(worker);
            consumerPool.submit(worker);
        }

        log.info("KafkaConsumerPool started: consumers={}, topics={}", consumerCount, topicsToSubscribe);
    }

    public void stopPolling() {
        workers.forEach(ConsumerWorker::stop);
    }

    public void commitAllOffsets() {
        consumers.forEach(c -> {
            try { c.commitSync(); } catch (Exception e) { log.warn("commitSync failed", e); }
        });
    }

    /**
     * Returns a snapshot of currently assigned partitions.
     * Safe to call from any thread — reads from ConcurrentHashMap cache,
     * never touches KafkaConsumer directly.
     */
    public Set<TopicPartition> getAssignedPartitions() {
        return new HashSet<>(assignmentCache);
    }

    /** Called by SafeRebalanceListener.onPartitionsAssigned() on the consumer thread. */
    void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignmentCache.addAll(partitions);
    }

    /** Called by SafeRebalanceListener.onPartitionsRevoked() on the consumer thread. */
    void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        assignmentCache.removeAll(partitions);
    }

    public void close() {
        stopPolling();
        consumerPool.shutdown();
        try { consumerPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        consumers.forEach(c -> { try { c.close(); } catch (Exception ignored) {} });
        flushPool.shutdown();
        try { flushPool.awaitTermination(60, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        log.info("KafkaConsumerPool closed");
    }

    private ThreadFactory namedThreadFactory(final String prefix) {
        AtomicInteger counter = new AtomicInteger(0);
        return r -> {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(false);
            return t;
        };
    }
}
