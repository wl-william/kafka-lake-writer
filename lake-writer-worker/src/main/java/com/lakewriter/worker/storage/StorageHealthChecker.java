package com.lakewriter.worker.storage;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Probes HDFS/OSS reachability at a configurable interval.
 *
 * Designed to be called on the consumer thread inside the poll loop.
 * Internally rate-limited: only one real I/O probe is issued per probeIntervalMs,
 * so it is safe to call on every poll iteration without hammering storage.
 *
 * Lifecycle:
 *   1. Flush fails permanently → ConsumerWorker pauses affected partitions.
 *   2. Each poll iteration calls probe(); returns cached result until interval elapses.
 *   3. When probe() returns true again → ConsumerWorker resumes paused partitions.
 *   4. If the committed offset expired during the outage, OffsetOutOfRangeException
 *      is caught by the poll loop and the partition is seeked to earliest (at-least-once).
 */
@Slf4j
public class StorageHealthChecker {

    private final StorageAdapter storage;
    private final String probePath;
    private final long probeIntervalMs;

    // Both fields are only accessed on the consumer thread — no synchronization needed.
    private boolean storageHealthy = true;
    private long lastProbeAtMs = 0;

    public StorageHealthChecker(StorageAdapter storage, String probePath, long probeIntervalMs) {
        this.storage = storage;
        this.probePath = probePath;
        this.probeIntervalMs = probeIntervalMs;
    }

    /**
     * Returns true if storage is reachable.
     *
     * Issues a real I/O probe at most once per probeIntervalMs.
     * Transitions healthy→unhealthy and unhealthy→healthy are logged at WARN/INFO.
     * Repeated unhealthy probes are suppressed to DEBUG to avoid log spam.
     */
    public boolean probe() {
        long now = System.currentTimeMillis();
        if (now - lastProbeAtMs < probeIntervalMs) {
            return storageHealthy;
        }
        lastProbeAtMs = now;
        try {
            storage.exists(probePath);
            if (!storageHealthy) {
                log.info("[StorageHealth] Storage recovered (probe path: {})", probePath);
                storageHealthy = true;
            }
        } catch (IOException e) {
            if (storageHealthy) {
                log.warn("[StorageHealth] Storage became unavailable (probe path: {}): {}",
                         probePath, e.getMessage());
            } else {
                log.debug("[StorageHealth] Storage still unavailable: {}", e.getMessage());
            }
            storageHealthy = false;
        }
        return storageHealthy;
    }

    public boolean isHealthy() {
        return storageHealthy;
    }
}
