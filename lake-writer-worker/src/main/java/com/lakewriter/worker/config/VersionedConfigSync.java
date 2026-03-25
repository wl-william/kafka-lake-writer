package com.lakewriter.worker.config;

import com.lakewriter.common.model.TopicSinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Delays subscription changes by a grace period after config.updatedAt.
 *
 * Problem: In multi-node deployments, each node polls independently.
 * Without delay, a new-topic config triggers one Rebalance per node as
 * each node polls and updates its subscription at different times.
 *
 * Solution: Wait 60 seconds from config.updatedAt before applying subscription
 * changes. By then, all nodes will have polled the same config and can
 * update their subscriptions within the same 30s window → one Rebalance.
 */
@Slf4j
public class VersionedConfigSync {

    /** Grace period in ms — should be > config poll interval (30s) */
    private static final long GRACE_PERIOD_MS = 60_000L;

    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "config-sync-scheduler");
            t.setDaemon(true);
            return t;
        });

    private final Consumer<TopicSinkConfig> applyFn;

    public VersionedConfigSync(Consumer<TopicSinkConfig> applySubscriptionChange) {
        this.applyFn = applySubscriptionChange;
    }

    /**
     * Schedule subscription change after grace period from config.updatedAt.
     * If grace period has already passed, apply immediately.
     */
    public void onConfigChanged(TopicSinkConfig newConfig) {
        long updatedAtMs = (newConfig.getUpdatedAt() != null)
            ? newConfig.getUpdatedAt().getTime()
            : System.currentTimeMillis();

        long effectiveTime = updatedAtMs + GRACE_PERIOD_MS;
        long delayMs = Math.max(0, effectiveTime - System.currentTimeMillis());

        if (delayMs == 0) {
            log.info("[VersionedSync] Grace period elapsed, applying immediately: topic={}", newConfig.getTopicName());
            applyFn.accept(newConfig);
        } else {
            log.info("[VersionedSync] Scheduling config in {}ms: topic={}", delayMs, newConfig.getTopicName());
            scheduler.schedule(() -> applyFn.accept(newConfig), delayMs, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
