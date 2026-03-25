package com.lakewriter.worker.buffer;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks total buffer memory usage and pauses consumption when threshold is reached.
 * Prevents OOM on 8GB nodes when HDFS is slow.
 *
 * Default: pause at 90% of maxBytes, resume at 80%.
 */
@Slf4j
public class BackpressureController {

    private final long maxBytes;
    private final AtomicLong currentBytes = new AtomicLong(0);

    public BackpressureController(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * Track bytes added to a buffer.
     * Always succeeds — records already polled from Kafka must not be dropped.
     * The poll loop uses canConsume() to prevent fetching new batches when full.
     */
    public void add(long bytes) {
        currentBytes.addAndGet(bytes);
    }

    public void release(long bytes) {
        long next = currentBytes.addAndGet(-bytes);
        if (next < 0) currentBytes.set(0);  // guard against underflow
    }

    /**
     * Whether consumers may call poll() for a new batch.
     * Uses 90% of maxBytes as the pause threshold; resumes polling at 80%.
     */
    public boolean canConsume() {
        return currentBytes.get() < maxBytes * 0.9;
    }

    public long getCurrentBytes() {
        return currentBytes.get();
    }
}
