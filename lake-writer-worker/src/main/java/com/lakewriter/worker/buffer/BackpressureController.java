package com.lakewriter.worker.buffer;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks total buffer memory usage and pauses consumption when threshold is reached.
 * Prevents OOM on 8GB nodes when HDFS is slow.
 *
 * Hysteresis: pause at 90% of maxBytes, resume only when usage drops below 80%.
 * Without hysteresis the poll loop oscillates: each flush releases a few MB,
 * briefly crossing back below 90%, triggering another poll batch that pushes
 * usage over 90% again — resulting in rapid stop/start cycling.
 */
@Slf4j
public class BackpressureController {

    private final long maxBytes;
    private final AtomicLong currentBytes = new AtomicLong(0);

    /**
     * Whether consumption is currently throttled.
     * Set to true when usage >= 90%; cleared only when usage falls below 80%.
     * Volatile is sufficient: worst-case race causes one extra poll batch, not correctness loss.
     */
    private volatile boolean throttled = false;

    public BackpressureController(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * Track bytes added to a buffer.
     * Always succeeds — records already polled from Kafka must not be dropped.
     * The poll loop uses canConsume() to prevent fetching new batches when full.
     */
    public void add(long bytes) {
        long total = currentBytes.addAndGet(bytes);
        if (total > maxBytes) {
            log.warn("[Backpressure] CRITICAL: buffer {}MB exceeds hard limit {}MB — " +
                     "flush threads may be stalled or storage unavailable",
                     total >> 20, maxBytes >> 20);
        }
    }

    public void release(long bytes) {
        long next = currentBytes.addAndGet(-bytes);
        if (next < 0) currentBytes.set(0);  // guard against underflow
    }

    /**
     * Whether consumers may call poll() for a new batch.
     * Pause threshold: 90% of maxBytes.
     * Resume threshold: 80% of maxBytes (hysteresis prevents rapid oscillation).
     */
    public boolean canConsume() {
        long current = currentBytes.get();
        if (!throttled && current >= maxBytes * 0.9) {
            throttled = true;
            log.warn("[Backpressure] Throttling consumers: buffer {}MB >= 90% of {}MB",
                     current >> 20, maxBytes >> 20);
        } else if (throttled && current < maxBytes * 0.8) {
            throttled = false;
            log.info("[Backpressure] Resuming consumers: buffer {}MB < 80% of {}MB",
                     current >> 20, maxBytes >> 20);
        }
        return !throttled;
    }

    /**
     * True when total buffer usage has exceeded the hard limit (100%).
     * Used by the poll loop to extend the sleep duration and reduce CPU spin
     * while waiting for flush threads to drain the backlog.
     */
    public boolean isCritical() {
        return currentBytes.get() >= maxBytes;
    }

    public long getCurrentBytes() {
        return currentBytes.get();
    }
}
