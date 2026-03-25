package com.lakewriter.worker.consumer;

import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.CheckpointManager;
import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.writer.FlushExecutor;
import com.lakewriter.worker.writer.FlushResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

/**
 * Single consumer thread — poll loop + flush trigger.
 *
 * Thread safety: KafkaConsumer is NOT thread-safe. All consumer operations
 * (poll, commitSync, seek) happen exclusively on this thread. Flush work is
 * submitted to flushPool, but commits are queued back to this thread via
 * pendingCommits and processed before each poll().
 *
 * 5-phase commit:
 *   Phase 1-3: done by FlushExecutor (write tmp → checkpoint → rename)
 *   Phase 4: commitSync — done on this thread from pendingCommits queue
 *   Phase 5: delete checkpoint — done on this thread after commitSync succeeds
 */
@Slf4j
public class ConsumerWorker implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final RecordDispatcher dispatcher;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final TopicMatcher topicMatcher;
    private final NodeIdentity nodeIdentity;
    private final CheckpointManager checkpointMgr;
    private final ExecutorService flushPool;

    /**
     * Queue of flush results waiting for commitSync on the consumer thread.
     * Flush threads post results here; the poll loop drains and commits.
     */
    private final ConcurrentLinkedQueue<PendingCommit> pendingCommits = new ConcurrentLinkedQueue<>();

    private volatile boolean running = true;

    public ConsumerWorker(KafkaConsumer<String, String> consumer,
                           RecordDispatcher dispatcher,
                           WriteBufferManager bufferManager,
                           FlushExecutor flushExecutor,
                           TopicMatcher topicMatcher,
                           NodeIdentity nodeIdentity,
                           CheckpointManager checkpointMgr,
                           ExecutorService flushPool) {
        this.consumer       = consumer;
        this.dispatcher     = dispatcher;
        this.bufferManager  = bufferManager;
        this.flushExecutor  = flushExecutor;
        this.topicMatcher   = topicMatcher;
        this.nodeIdentity   = nodeIdentity;
        this.checkpointMgr  = checkpointMgr;
        this.flushPool      = flushPool;
    }

    @Override
    public void run() {
        log.info("ConsumerWorker started, thread={}", Thread.currentThread().getName());
        try {
            while (running) {
                // Phase 4+5: process any completed flush results (commit + delete checkpoint)
                drainPendingCommits();

                // Backpressure: pause polling if buffer is full
                if (!bufferManager.getBackpressure().canConsume()) {
                    try { Thread.sleep(100); } catch (InterruptedException e) { break; }
                    continue;
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    dispatcher.dispatch(record);
                }

                // Check all assigned partitions for flush triggers
                checkAndTriggerFlush();
            }
        } catch (Exception e) {
            log.error("ConsumerWorker error: {}", e.getMessage(), e);
        } finally {
            log.info("ConsumerWorker stopped");
        }
    }

    /**
     * Drain and commit all pending flush results on the consumer thread.
     * Phase 4 (commitSync) then Phase 5 (delete checkpoint) per the 5-phase protocol.
     */
    private void drainPendingCommits() {
        PendingCommit pending;
        while ((pending = pendingCommits.poll()) != null) {
            final PendingCommit p = pending;
            try {
                // Phase 4: commitSync (safe — on consumer thread)
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(p.tp, new OffsetAndMetadata(p.toFlush.getLastOffset() + 1));
                consumer.commitSync(offsets);
                log.debug("[Commit] Phase 4 complete: committed offset {} for {}", p.toFlush.getLastOffset() + 1, p.tp);

                // Phase 5: delete checkpoint after commit succeeds
                checkpointMgr.delete(p.tp);
                log.debug("[Commit] Phase 5 complete: checkpoint deleted for {}", p.tp);

                bufferManager.getBackpressure().release(p.toFlush.getTotalBytes());
                p.dbuf.recycleFlushed(p.toFlush);
            } catch (Exception e) {
                log.error("[Commit] Phase 4 failed for {}: {} — stopping drain to preserve ordering",
                    p.tp, e.getMessage(), e);
                // Stop draining: if we skip this TP's commit and process the next one for the
                // same TP, the later offset would overwrite the failed one, causing data loss.
                // Remaining items stay in the queue and will be retried on the next poll iteration.
                break;
            }
        }
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    /**
     * Check all assigned partitions for flush thresholds.
     * Flush is submitted to a separate thread pool; result is queued back to consumer thread.
     */
    private void checkAndTriggerFlush() {
        for (TopicPartition tp : consumer.assignment()) {
            DoubleWriteBuffer dbuf = bufferManager.getBuffer(tp);
            if (dbuf == null || !dbuf.getActiveBuffer().shouldFlush()) continue;

            TopicSinkConfig config = topicMatcher.match(tp.topic());
            if (config == null) continue;

            WriteBuffer toFlush = dbuf.swapForFlush();
            final TopicPartition tpFinal = tp;

            flushPool.submit(() -> {
                FlushResult result = flushExecutor.flush(
                    toFlush, config.parseSchema(), nodeIdentity.getNodeId());
                if (result.isSuccess() && result.getTargetFilePath() != null) {
                    // Queue commit back to consumer thread (Phase 4+5)
                    pendingCommits.add(new PendingCommit(tpFinal, toFlush, dbuf));
                } else if (result.isSuccess()) {
                    // Skipped flush (empty or already written) — just recycle the buffer
                    dbuf.recycleFlushed(toFlush);
                } else {
                    log.error("[Flush] Failed for {}: {}", tpFinal, result.getErrorMessage());
                    // Recycle buffer so standby is available; data lost in this batch
                    // but checkpoint was saved — on restart recovery will re-seek
                    dbuf.recycleFlushed(toFlush);
                }
            });
        }
    }

    /** Carries flush result context needed for Phase 4+5 on consumer thread. */
    private static class PendingCommit {
        final TopicPartition tp;
        final WriteBuffer toFlush;
        final DoubleWriteBuffer dbuf;

        PendingCommit(TopicPartition tp, WriteBuffer toFlush, DoubleWriteBuffer dbuf) {
            this.tp      = tp;
            this.toFlush = toFlush;
            this.dbuf    = dbuf;
        }
    }
}
