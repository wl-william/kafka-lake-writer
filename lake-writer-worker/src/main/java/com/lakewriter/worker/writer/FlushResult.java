package com.lakewriter.worker.writer;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

/**
 * Result of a flush operation.
 * On success, topicPartition is provided so the caller can delete the checkpoint
 * AFTER commitSync succeeds (Phase 5 of the 5-phase commit protocol).
 */
@Getter
public class FlushResult {

    private final boolean success;
    private final String targetFilePath;
    private final int rowCount;
    private final String errorMessage;
    /** Non-null on success — used by caller to delete checkpoint after commitSync (Phase 5). */
    private final TopicPartition topicPartition;

    private FlushResult(boolean success, String targetFilePath, int rowCount,
                        String errorMessage, TopicPartition topicPartition) {
        this.success = success;
        this.targetFilePath = targetFilePath;
        this.rowCount = rowCount;
        this.errorMessage = errorMessage;
        this.topicPartition = topicPartition;
    }

    public static FlushResult success(String targetFilePath, int rowCount, TopicPartition tp) {
        return new FlushResult(true, targetFilePath, rowCount, null, tp);
    }

    /** Used when flush is skipped (empty buffer or already written). */
    public static FlushResult skipped(TopicPartition tp) {
        return new FlushResult(true, null, 0, null, tp);
    }

    public static FlushResult failed(String reason) {
        return new FlushResult(false, null, 0, reason, null);
    }
}
