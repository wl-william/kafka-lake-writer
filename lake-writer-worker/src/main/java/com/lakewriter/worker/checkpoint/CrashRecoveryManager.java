package com.lakewriter.worker.checkpoint;

import com.lakewriter.worker.storage.StorageAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles all 7 crash scenarios by inspecting checkpoint files and HDFS state at startup.
 *
 * C-1/C-2: No checkpoint → consumer restarts from committed offset automatically
 * C-3/C-4: Checkpoint + no HDFS files → roll back to startOffset
 * C-5:     Checkpoint + tmp file exists → complete rename → seek to endOffset+1
 * C-6/C-7: Checkpoint + target file exists → already done → seek to endOffset+1
 */
@Slf4j
public class CrashRecoveryManager {

    private final CheckpointManager checkpointMgr;
    private final StorageAdapter storage;

    public CrashRecoveryManager(String checkpointDir, StorageAdapter storage) {
        this.checkpointMgr = new CheckpointManager(checkpointDir);
        this.storage = storage;
    }

    public CrashRecoveryManager(CheckpointManager checkpointMgr, StorageAdapter storage) {
        this.checkpointMgr = checkpointMgr;
        this.storage = storage;
    }

    /**
     * Called at process startup before any Kafka polling begins.
     * @return map of TopicPartition → seek offset (where to resume consuming)
     */
    public Map<TopicPartition, Long> recover() {
        Map<TopicPartition, Long> seekOffsets = new HashMap<>();
        List<Checkpoint> checkpoints = checkpointMgr.loadAll();

        if (checkpoints.isEmpty()) {
            log.info("[Recovery] No checkpoint files found — starting from committed offsets");
            return seekOffsets;
        }

        log.info("[Recovery] Found {} checkpoint(s), starting recovery", checkpoints.size());

        for (Checkpoint ckpt : checkpoints) {
            TopicPartition tp = new TopicPartition(ckpt.getTopic(), ckpt.getPartition());
            long seekTo = recoverOne(ckpt, tp);
            seekOffsets.put(tp, seekTo);
            checkpointMgr.delete(tp);
        }

        log.info("[Recovery] Recovery complete. Seek map: {}", seekOffsets);
        return seekOffsets;
    }

    /**
     * Recover a single partition — returns the offset to seek to.
     */
    public long recoverPartition(Checkpoint ckpt) {
        TopicPartition tp = new TopicPartition(ckpt.getTopic(), ckpt.getPartition());
        long seekTo = recoverOne(ckpt, tp);
        checkpointMgr.delete(tp);
        return seekTo;
    }

    private long recoverOne(Checkpoint ckpt, TopicPartition tp) {
        try {
            if (storage.exists(ckpt.getTargetFilePath())) {
                // C-6/C-7: target file already exists — flush was complete
                log.info("[Recovery] C-6/C-7 target exists: {} → seek to {}", tp, ckpt.getEndOffset() + 1);
                return ckpt.getEndOffset() + 1;

            } else if (ckpt.getTmpFilePath() != null && storage.exists(ckpt.getTmpFilePath())) {
                // C-5: tmp file exists but rename wasn't done — complete it now
                log.info("[Recovery] C-5 completing rename: {} → {}", ckpt.getTmpFilePath(), ckpt.getTargetFilePath());
                storage.rename(ckpt.getTmpFilePath(), ckpt.getTargetFilePath());
                log.info("[Recovery] C-5 rename done: {} → seek to {}", tp, ckpt.getEndOffset() + 1);
                return ckpt.getEndOffset() + 1;

            } else {
                // C-3/C-4: no files on HDFS — data was never written, re-consume
                log.info("[Recovery] C-3/C-4 no HDFS file: {} → rollback to {}", tp, ckpt.getStartOffset());
                return ckpt.getStartOffset();
            }
        } catch (IOException e) {
            // HDFS unavailable — conservative: roll back to re-consume
            log.warn("[Recovery] HDFS check failed for {} — rolling back to {}", tp, ckpt.getStartOffset(), e);
            return ckpt.getStartOffset();
        }
    }
}
