package com.lakewriter.worker.buffer;

import com.lakewriter.common.model.TopicSinkConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Double-buffer (swap) pattern for non-blocking consumption during flush.
 *
 * While active buffer is being written by consumer thread,
 * flushing buffer can be sent to HDFS concurrently.
 * After flush completes, buffers swap roles.
 */
public class DoubleWriteBuffer {

    private volatile WriteBuffer active;
    private volatile WriteBuffer standby;
    private final ReentrantLock swapLock = new ReentrantLock();
    private final TopicPartition topicPartition;
    private final TopicSinkConfig config;

    public DoubleWriteBuffer(TopicPartition topicPartition, TopicSinkConfig config) {
        this.topicPartition = topicPartition;
        this.config = config;
        this.active  = new WriteBuffer(topicPartition, config);
        this.standby = new WriteBuffer(topicPartition, config);
    }

    /** Consumer thread appends to active buffer — no lock needed (single writer) */
    public void append(Object[] row, long offset, long estimatedBytes) {
        active.append(row, offset, estimatedBytes);
    }

    public WriteBuffer getActiveBuffer() {
        return active;
    }

    /**
     * Atomically swap active ↔ standby, return the buffer to flush.
     * After this call, consumer continues writing to the new active buffer.
     *
     * If standby is null (previous flush still in progress), creates a fresh
     * buffer instead of setting active to null, preventing NPE on subsequent appends.
     */
    public WriteBuffer swapForFlush() {
        swapLock.lock();
        try {
            WriteBuffer toFlush = active;
            active  = (standby != null) ? standby : new WriteBuffer(topicPartition, config);
            standby = null;
            return toFlush;
        } finally {
            swapLock.unlock();
        }
    }

    /**
     * Return flushed (now empty) buffer as the new standby.
     */
    public void recycleFlushed(WriteBuffer flushed) {
        flushed.clear();
        swapLock.lock();
        try {
            standby = flushed;
        } finally {
            swapLock.unlock();
        }
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
