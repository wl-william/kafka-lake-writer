package com.lakewriter.worker.consumer;

import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.Checkpoint;
import com.lakewriter.worker.checkpoint.CheckpointManager;
import com.lakewriter.worker.checkpoint.CrashRecoveryManager;
import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.writer.FlushExecutor;
import com.lakewriter.worker.writer.FlushResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Handles Kafka partition rebalance events safely.
 *
 * onPartitionsRevoked:  flush all affected buffers before yielding partitions
 * onPartitionsAssigned: check for crash checkpoints and recover if needed
 */
@Slf4j
public class SafeRebalanceListener implements ConsumerRebalanceListener {

    private final WriteBufferManager bufferManager;
    private final CheckpointManager checkpointMgr;
    private final CrashRecoveryManager recoveryMgr;
    private final FlushExecutor flushExecutor;
    private final TopicMatcher topicMatcher;
    private final NodeIdentity nodeIdentity;
    private final KafkaConsumer<String, String> consumer;

    public SafeRebalanceListener(WriteBufferManager bufferManager,
                                  CheckpointManager checkpointMgr,
                                  CrashRecoveryManager recoveryMgr,
                                  FlushExecutor flushExecutor,
                                  TopicMatcher topicMatcher,
                                  NodeIdentity nodeIdentity,
                                  KafkaConsumer<String, String> consumer) {
        this.bufferManager = bufferManager;
        this.checkpointMgr = checkpointMgr;
        this.recoveryMgr   = recoveryMgr;
        this.flushExecutor = flushExecutor;
        this.topicMatcher  = topicMatcher;
        this.nodeIdentity  = nodeIdentity;
        this.consumer      = consumer;
    }

    /**
     * Called before partitions are revoked — emergency flush all affected buffers.
     * If flush fails, offset is not committed; new owner will re-consume from last commit.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        long start = System.currentTimeMillis();
        log.info("[Rebalance] Revoking {} partitions: {}", partitions.size(), partitions);

        int success = 0, failed = 0;
        for (TopicPartition tp : partitions) {
            DoubleWriteBuffer dbuf = bufferManager.getBuffer(tp);
            if (dbuf == null || dbuf.getActiveBuffer().isEmpty()) {
                bufferManager.removeBuffer(tp);
                continue;
            }
            TopicSinkConfig config = topicMatcher.match(tp.topic());
            if (config == null) { bufferManager.removeBuffer(tp); continue; }

            try {
                WriteBuffer toFlush = dbuf.swapForFlush();
                // Phase 1-3
                FlushResult result = flushExecutor.flush(toFlush, config.parseSchema(), nodeIdentity.getNodeId());
                if (result.isSuccess()) {
                    // Phase 4: commitSync
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(tp, new OffsetAndMetadata(toFlush.getLastOffset() + 1));
                    consumer.commitSync(offsets);
                    // Phase 5: delete checkpoint only after commit succeeds
                    checkpointMgr.delete(tp);
                    bufferManager.getBackpressure().release(toFlush.getTotalBytes());
                    success++;
                } else {
                    failed++;
                    log.warn("[Rebalance] Flush failed for {}: {}", tp, result.getErrorMessage());
                    // Leave checkpoint on disk — new partition owner will recover via onPartitionsAssigned
                }
            } catch (Exception e) {
                failed++;
                log.warn("[Rebalance] Emergency flush error for {}: {}", tp, e.getMessage());
            }
            bufferManager.removeBuffer(tp);
        }

        log.info("[Rebalance] Revoke done: success={}, failed={}, elapsed={}ms",
            success, failed, System.currentTimeMillis() - start);
    }

    /**
     * Called after partitions are assigned — check for crash checkpoints, create buffers.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        log.info("[Rebalance] Assigned {} partitions: {}", partitions.size(), partitions);

        for (TopicPartition tp : partitions) {
            // Check for leftover checkpoint (this node previously owned this partition)
            Optional<Checkpoint> ckpt = checkpointMgr.load(tp);
            if (ckpt.isPresent()) {
                long seekTo = recoveryMgr.recoverPartition(ckpt.get());
                consumer.seek(tp, seekTo);
                log.info("[Rebalance] Recovered checkpoint for {} → seek to {}", tp, seekTo);
            }

            // Create buffer for the new partition
            TopicSinkConfig config = topicMatcher.match(tp.topic());
            if (config != null) {
                bufferManager.createBuffer(tp, config);
            }
        }
    }
}
