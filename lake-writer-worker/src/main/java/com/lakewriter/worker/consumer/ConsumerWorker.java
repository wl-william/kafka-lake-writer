package com.lakewriter.worker.consumer;

import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.checkpoint.RemoteCheckpointManager;
import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.storage.StorageHealthChecker;
import com.lakewriter.worker.writer.FlushExecutor;
import com.lakewriter.worker.writer.FlushResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private final RemoteCheckpointManager checkpointMgr;
    private final ExecutorService flushPool;
    private final StorageHealthChecker storageHealthChecker;

    /**
     * Queue of flush results waiting for commitSync on the consumer thread.
     * Flush threads post results here; the poll loop drains and commits.
     */
    private final ConcurrentLinkedQueue<PendingCommit> pendingCommits = new ConcurrentLinkedQueue<>();

    /**
     * Cross-thread signal: flush thread enqueues a partition when all retries are exhausted.
     * Consumer thread drains this queue before each poll() and moves entries to pausedDueToFailure.
     */
    private final ConcurrentLinkedQueue<TopicPartition> failedFlushSignals = new ConcurrentLinkedQueue<>();

    /**
     * Partitions currently paused because of a permanent flush failure.
     * Accessed only on the consumer thread — no synchronization needed.
     * When StorageHealthChecker.probe() returns true, all entries are resumed.
     */
    private final Set<TopicPartition> pausedDueToFailure = new HashSet<>();

    private volatile boolean running = true;

    public ConsumerWorker(KafkaConsumer<String, String> consumer,
                           RecordDispatcher dispatcher,
                           WriteBufferManager bufferManager,
                           FlushExecutor flushExecutor,
                           TopicMatcher topicMatcher,
                           NodeIdentity nodeIdentity,
                           RemoteCheckpointManager checkpointMgr,
                           ExecutorService flushPool,
                           StorageHealthChecker storageHealthChecker) {
        this.consumer             = consumer;
        this.dispatcher           = dispatcher;
        this.bufferManager        = bufferManager;
        this.flushExecutor        = flushExecutor;
        this.topicMatcher         = topicMatcher;
        this.nodeIdentity         = nodeIdentity;
        this.checkpointMgr        = checkpointMgr;
        this.flushPool            = flushPool;
        this.storageHealthChecker = storageHealthChecker;
    }

    @Override
    public void run() {
        log.info("ConsumerWorker started, thread={}", Thread.currentThread().getName());
        try {
            while (running) {
                // Phase 4+5: process any completed flush results (commit + delete checkpoint)
                drainPendingCommits();

                // Move flush-failure signals into pausedDueToFailure and call consumer.pause().
                // Must run before poll() so no new records are fetched for failing partitions.
                drainFlushFailureSignals();

                // If any partitions are paused due to storage failure, probe for recovery.
                // When storage comes back, resume them; if their offset expired, the
                // OffsetOutOfRangeException handler below will seek to earliest.
                checkStorageRecovery();

                // Backpressure: slow down polling if buffer is approaching the memory limit
                if (!bufferManager.getBackpressure().canConsume()) {
                    long sleepMs = bufferManager.getBackpressure().isCritical() ? 500 : 100;
                    try { Thread.sleep(sleepMs); } catch (InterruptedException e) { break; }
                    continue;
                }

                ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (OffsetOutOfRangeException e) {
                    // Checkpoint recovery (or post-pause resume) seeked to an offset Kafka has
                    // already pruned. Seek to earliest to avoid a permanent stall.
                    // At-least-once: re-consuming old data is safe; skipping is not.
                    handleOffsetOutOfRange(e);
                    continue;
                }
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

            // Skip commits for partitions no longer assigned to this consumer (revoked during
            // rebalance). Attempting commitSync on a revoked partition throws an exception and
            // would stop the drain loop, blocking commits for all still-active partitions.
            // The remote checkpoint left by the flush will allow the new owner to recover.
            if (!consumer.assignment().contains(p.tp)) {
                log.debug("[Commit] Skipping commit for revoked partition {} — new owner will recover via checkpoint", p.tp);
                bufferManager.getBackpressure().release(p.toFlush.getTotalBytes());
                p.dbuf.recycleFlushed(p.toFlush);
                continue;
            }

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
     * Seek affected partitions to their earliest available Kafka offset.
     * Called when the offset stored in a checkpoint (or the position after a long pause)
     * has been pruned by Kafka log retention.
     * At-least-once: re-consuming old data is safe; skipping is not.
     */
    private void handleOffsetOutOfRange(OffsetOutOfRangeException e) {
        log.warn("[Consumer] OffsetOutOfRange: offsets pruned by Kafka. " +
                 "Seeking to earliest to avoid permanent stall. Partitions: {}", e.partitions());
        Map<TopicPartition, Long> earliest = consumer.beginningOffsets(e.partitions());
        for (Map.Entry<TopicPartition, Long> entry : earliest.entrySet()) {
            log.warn("[Consumer] {} → seek to earliest={}", entry.getKey(), entry.getValue());
            consumer.seek(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Drain flush-failure signals posted by flush threads and pause the affected partitions.
     * Must run on the consumer thread — KafkaConsumer.pause() is not thread-safe.
     *
     * Pausing prevents subsequent successful flushes from committing offsets that skip over
     * the failed range.  Because the failed batch's offset was never committed, Kafka still
     * holds those records; they will be re-delivered once storage recovers and we resume.
     */
    private void drainFlushFailureSignals() {
        TopicPartition tp;
        while ((tp = failedFlushSignals.poll()) != null) {
            if (pausedDueToFailure.add(tp)) {
                consumer.pause(Collections.singleton(tp));
                log.error("[Consumer] Partition {} PAUSED — permanent flush failure. " +
                          "Will auto-resume when storage recovers.", tp);
            }
        }
    }

    /**
     * When partitions are paused due to a storage failure, periodically probe the storage.
     * StorageHealthChecker is rate-limited internally, so this is safe to call every iteration.
     *
     * On recovery:
     *   consumer.resume() → next poll fetches from last committed offset.
     *   If that offset expired while storage was down, OffsetOutOfRangeException is caught
     *   above and the partition seeks to the earliest available offset (at-least-once).
     */
    private void checkStorageRecovery() {
        if (pausedDueToFailure.isEmpty()) return;
        if (!storageHealthChecker.probe()) return;

        // Intersect with consumer.paused() to avoid resuming revoked partitions
        Set<TopicPartition> currentlyPaused = consumer.paused();
        Set<TopicPartition> toResume = new HashSet<>(pausedDueToFailure);
        toResume.retainAll(currentlyPaused);

        if (!toResume.isEmpty()) {
            consumer.resume(toResume);
            log.info("[Consumer] Storage recovered — resuming {} partition(s): {}", toResume.size(), toResume);
        }
        pausedDueToFailure.clear();
    }

    /**
     * Check all assigned partitions for flush thresholds.
     * Flush is submitted to a separate thread pool; result is queued back to consumer thread.
     */
    private void checkAndTriggerFlush() {
        for (TopicPartition tp : consumer.assignment()) {
            DoubleWriteBuffer dbuf = bufferManager.getBuffer(tp);
            if (dbuf == null || !dbuf.getActiveBuffer().shouldFlush()) continue;

            // Guard: skip if a flush is already in flight for this partition.
            // If we swapped again while standby == null, the two flush threads would race
            // to add PendingCommits in completion order (not offset order), causing the
            // consumer thread to commit offsets out of sequence for the same partition.
            if (dbuf.isFlushInProgress()) {
                log.debug("[Flush] Skipping trigger for {} — previous flush still in progress", tp);
                continue;
            }

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
                    log.error("[Flush] Permanent failure for {} offsets {}-{}: {}. " +
                              "Partition will be paused; Kafka will re-deliver from last committed offset on restart.",
                              tpFinal, toFlush.getStartOffset(), toFlush.getLastOffset(),
                              result.getErrorMessage());
                    // Signal consumer thread to pause this partition before the next poll().
                    // This prevents a later successful flush from committing an offset that
                    // jumps over the failed range, which would cause permanent data loss.
                    failedFlushSignals.add(tpFinal);
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
