package com.lakewriter.worker.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prometheus metrics for Worker.
 * Exposed via /actuator/prometheus (port 8081).
 *
 * Metrics naming follows Prometheus conventions: kafka_lake_writer_*
 */
@Slf4j
@Component
public class WriterMetrics {

    private final MeterRegistry registry;
    private final AtomicLong recentRecords = new AtomicLong(0);
    private volatile long lastRecordCountSnapshot = 0;
    private volatile long lastSnapshotTimeMs = System.currentTimeMillis();

    public WriterMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordConsumed(String topic, int count) {
        registry.counter("kafka_lake_writer_records_consumed_total", "topic", topic)
                .increment(count);
        recentRecords.addAndGet(count);
    }

    public void recordFlushed(String topic, int count, long durationMs) {
        registry.counter("kafka_lake_writer_records_flushed_total", "topic", topic)
                .increment(count);
        registry.timer("kafka_lake_writer_flush_duration_ms", "topic", topic)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordFlushFailure(String topic) {
        registry.counter("kafka_lake_writer_flush_failure_total", "topic", topic).increment();
    }

    public void recordFlushRetry(String topic) {
        registry.counter("kafka_lake_writer_flush_retry_total", "topic", topic).increment();
    }

    public void recordParseError() {
        registry.counter("kafka_lake_writer_parse_error_total").increment();
    }

    public void recordRebalance(String event) {
        registry.counter("kafka_lake_writer_rebalance_count", "event", event).increment();
    }

    public void recordBufferUsage(String topic, double ratio) {
        registry.gauge("kafka_lake_writer_buffer_usage_ratio",
            java.util.Collections.singletonList(io.micrometer.core.instrument.Tag.of("topic", topic)),
            ratio);
    }

    /** Approximate records/s over the last reporting interval */
    public double getRecentRecordsPerSec() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastSnapshotTimeMs;
        if (elapsed < 1000) return 0;
        long current = recentRecords.get();
        double rps = (double)(current - lastRecordCountSnapshot) / elapsed * 1000;
        lastRecordCountSnapshot = current;
        lastSnapshotTimeMs = now;
        return Math.max(0, rps);
    }
}
