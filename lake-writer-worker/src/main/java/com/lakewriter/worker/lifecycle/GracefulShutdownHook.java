package com.lakewriter.worker.lifecycle;

import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.RemoteCheckpointManager;
import com.lakewriter.worker.consumer.KafkaConsumerPool;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.storage.StorageAdapter;
import com.lakewriter.worker.writer.FlushExecutor;
import com.lakewriter.worker.writer.FlushResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles SIGTERM / SIGINT graceful shutdown.
 *
 * Shutdown sequence:
 *   1. Stop consumer poll loops
 *   2. Flush all remaining buffers to HDFS
 *   3. Commit all offsets
 *   4. Close Kafka consumers
 *   5. Close HDFS connections
 *   6. Thread pools terminate naturally
 */
@Slf4j
@Component
public class GracefulShutdownHook {

    private final KafkaConsumerPool consumerPool;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final RemoteCheckpointManager checkpointMgr;
    private final NodeIdentity nodeIdentity;
    private final StorageAdapter storage;

    public GracefulShutdownHook(KafkaConsumerPool consumerPool,
                                 WriteBufferManager bufferManager,
                                 FlushExecutor flushExecutor,
                                 RemoteCheckpointManager checkpointMgr,
                                 NodeIdentity nodeIdentity,
                                 StorageAdapter storage) {
        this.consumerPool  = consumerPool;
        this.bufferManager = bufferManager;
        this.flushExecutor = flushExecutor;
        this.checkpointMgr = checkpointMgr;
        this.nodeIdentity  = nodeIdentity;
        this.storage       = storage;
    }

    @PreDestroy
    public void shutdown() {
        log.info("=== Graceful shutdown initiated ===");

        // Step 1: stop consumers from polling new records
        log.info("[Shutdown] Step 1: Stopping consumer poll loops...");
        consumerPool.stopPolling();

        // Step 2: flush all remaining buffers (Phase 1-3)
        // Collect successfully flushed partitions for Phase 5 after commit.
        log.info("[Shutdown] Step 2: Flushing all buffers...");
        int flushed = 0;
        List<TopicPartition> flushedPartitions = new ArrayList<>();
        List<DoubleWriteBuffer> all = bufferManager.getAllBuffers();
        for (DoubleWriteBuffer dbuf : all) {
            WriteBuffer toFlush = dbuf.swapForFlush();
            if (!toFlush.isEmpty()) {
                try {
                    FlushResult result = flushExecutor.flush(
                        toFlush, toFlush.getConfig().parseSchema(), nodeIdentity.getNodeId());
                    if (result.isSuccess() && result.getTargetFilePath() != null) {
                        flushedPartitions.add(toFlush.getTopicPartition());
                        flushed++;
                    }
                } catch (Exception e) {
                    log.error("[Shutdown] Emergency flush failed for {}: {}",
                        toFlush.getTopicPartition(), e.getMessage());
                }
            }
        }
        log.info("[Shutdown] Step 2: Flushed {} buffers", flushed);

        // Step 3: commit all offsets (Phase 4)
        log.info("[Shutdown] Step 3: Committing offsets...");
        try {
            consumerPool.commitAllOffsets();
        } catch (Exception e) {
            log.warn("[Shutdown] Offset commit error: {}", e.getMessage());
        }

        // Phase 5: delete remote checkpoints for partitions we successfully flushed and committed.
        // Without this, the next owner (after rebalance) would find stale checkpoints and
        // perform unnecessary crash recovery, seeking to endOffset+1 instead of continuing normally.
        log.info("[Shutdown] Phase 5: Deleting {} remote checkpoint(s)...", flushedPartitions.size());
        for (TopicPartition tp : flushedPartitions) {
            checkpointMgr.delete(tp);
        }

        // Step 4: close consumers (sends LeaveGroup → fast Rebalance on remaining nodes)
        log.info("[Shutdown] Step 4: Closing Kafka consumers...");
        consumerPool.close();

        // Step 5: close storage
        log.info("[Shutdown] Step 5: Closing storage...");
        try {
            storage.close();
        } catch (Exception e) {
            log.warn("[Shutdown] Storage close error: {}", e.getMessage());
        }

        log.info("=== Graceful shutdown complete ===");
    }
}
