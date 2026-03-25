package com.lakewriter.worker.writer;

import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.checkpoint.Checkpoint;
import com.lakewriter.worker.checkpoint.CheckpointManager;
import com.lakewriter.worker.checkpoint.IdempotentWriteChecker;
import com.lakewriter.worker.schema.PathResolver;
import com.lakewriter.worker.storage.StorageAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Executes the 5-phase commit flush:
 *   Phase 1: Write data to tmp file on HDFS
 *   Phase 2: Write checkpoint to local disk
 *   Phase 3: Atomic rename tmp → target (HDFS rename is atomic)
 *   Phase 4: commitSync offset (done by caller after return)
 *   Phase 5: Delete checkpoint file
 *
 * Retry: 3 attempts with exponential backoff (2s, 4s, 8s).
 * On persistent failure: caller should enter degraded mode (pause partition).
 */
@Slf4j
public class FlushExecutor {

    private static final int MAX_RETRIES = 3;

    private final StorageAdapter storage;
    private final CheckpointManager checkpointMgr;
    private final FileWriterFactory writerFactory;
    private final IdempotentWriteChecker idempotentChecker;

    public FlushExecutor(StorageAdapter storage, CheckpointManager checkpointMgr,
                         IdempotentWriteChecker idempotentChecker) {
        this.storage = storage;
        this.checkpointMgr = checkpointMgr;
        this.writerFactory = new FileWriterFactory(storage);
        this.idempotentChecker = idempotentChecker;
    }

    /**
     * Flush buffer to HDFS with retry. Executes Phases 1-3 only.
     *
     * Phase 4 (commitSync) and Phase 5 (delete checkpoint) are the CALLER's responsibility,
     * in that order. The returned FlushResult carries the TopicPartition so the caller can
     * delete the correct checkpoint file after commitSync succeeds.
     */
    public FlushResult flush(WriteBuffer buffer, SchemaDefinition schema, String nodeId) {
        if (buffer.isEmpty()) {
            return FlushResult.skipped(buffer.getTopicPartition());
        }

        TopicPartition tp = buffer.getTopicPartition();

        // Idempotent check: skip if this offset range was already written
        if (idempotentChecker != null
                && idempotentChecker.isAlreadyWritten(tp, buffer.getStartOffset(), buffer.getLastOffset())) {
            log.info("[Flush] Skipping already-written range {}-{} for {}",
                buffer.getStartOffset(), buffer.getLastOffset(), tp);
            return FlushResult.skipped(tp);
        }

        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                FlushResult result = doFlush(buffer, schema, nodeId);
                if (idempotentChecker != null) {
                    idempotentChecker.recordWritten(tp, buffer.getStartOffset(), buffer.getLastOffset());
                }
                return result;
            } catch (IOException e) {
                attempt++;
                log.warn("[Flush] Attempt {}/{} failed for {}: {}", attempt, MAX_RETRIES, tp, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    sleepMs(2000L * (1L << (attempt - 1)));   // 2s, 4s
                }
            }
        }

        log.error("[Flush] All {} retries exhausted for {}", MAX_RETRIES, tp);
        return FlushResult.failed("Max retries exceeded for " + tp);
    }

    private FlushResult doFlush(WriteBuffer buffer, SchemaDefinition schema, String nodeId) throws IOException {
        TopicPartition tp = buffer.getTopicPartition();
        TopicSinkConfig config = buffer.getConfig();

        // Resolve path template (e.g. /hdfs/data/orders/2026-03-22)
        String resolvedPath = PathResolver.resolve(config.getSinkPath(), tp.topic(), null);

        // Build file names
        String ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String ext = config.getCompression().toLowerCase() + "." + config.getSinkFormat().toLowerCase();
        String fileName = String.format("part-%s-P%d-%dto%d-%s.%s",
            nodeId, tp.partition(), buffer.getStartOffset(), buffer.getLastOffset(), ts, ext);

        String tmpPath    = resolvedPath + "/_tmp/" + fileName.replace("." + ext, ".tmp");
        String targetPath = resolvedPath + "/" + fileName;

        List<Object[]> rows = buffer.drainRows();

        // Phase 1: write to tmp file
        try (FormatWriter writer = writerFactory.create(config)) {
            writer.open(tmpPath, schema, config);
            writer.writeRows(rows);
        }
        log.debug("[Flush] Phase 1 complete: wrote {} rows to {}", rows.size(), tmpPath);

        // Phase 2: write checkpoint to local disk
        Checkpoint ckpt = new Checkpoint(tp.topic(), tp.partition(),
            buffer.getStartOffset(), buffer.getLastOffset(), rows.size(),
            tmpPath, targetPath, nodeId);
        checkpointMgr.save(ckpt);
        log.debug("[Flush] Phase 2 complete: checkpoint saved");

        // Phase 3: atomic rename
        storage.rename(tmpPath, targetPath);
        log.info("[Flush] Phase 3 complete (Phases 1-3 done): renamed to {}", targetPath);

        // Phase 4 (commitSync) and Phase 5 (delete checkpoint) are done by the caller
        // in that exact order to preserve At-Least-Once and crash-recovery guarantees.
        return FlushResult.success(targetPath, rows.size(), tp);
    }

    private void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
