package com.lakewriter.worker.buffer;

import com.lakewriter.common.model.TopicSinkConfig;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates rows for a single TopicPartition before a flush.
 * Tracks offset range and total bytes to determine flush triggers.
 *
 * Thread-safety: single consumer thread writes; flush thread reads only after swap.
 */
public class WriteBuffer {

    @Getter private final TopicPartition topicPartition;
    @Getter private final TopicSinkConfig config;

    private final List<Object[]> rows = new ArrayList<>();
    @Getter private long startOffset = -1;
    @Getter private long lastOffset  = -1;
    @Getter private long totalBytes  = 0;

    private long createdAtMs = System.currentTimeMillis();

    public WriteBuffer(TopicPartition topicPartition, TopicSinkConfig config) {
        this.topicPartition = topicPartition;
        this.config = config;
    }

    public void append(Object[] row, long offset, long estimatedBytes) {
        rows.add(row);
        if (startOffset == -1) startOffset = offset;
        lastOffset = offset;
        totalBytes += estimatedBytes;
    }

    public boolean shouldFlush() {
        if (rows.isEmpty()) return false;
        long ageMs = System.currentTimeMillis() - createdAtMs;
        return rows.size() >= config.getFlushRows()
            || totalBytes >= config.getFlushBytes()
            || ageMs >= config.getFlushIntervalSec() * 1000L;
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public int getRowCount() {
        return rows.size();
    }

    /** Returns a snapshot of accumulated rows for flushing */
    public List<Object[]> drainRows() {
        return new ArrayList<>(rows);
    }

    public void clear() {
        rows.clear();
        startOffset = -1;
        lastOffset  = -1;
        totalBytes  = 0;
        createdAtMs = System.currentTimeMillis();
    }
}
