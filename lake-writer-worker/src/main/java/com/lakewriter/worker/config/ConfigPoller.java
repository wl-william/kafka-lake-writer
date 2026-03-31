package com.lakewriter.worker.config;

import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.common.repository.TopicSinkConfigRepository;
import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.node.NodeIdentity;
import com.lakewriter.worker.writer.FlushExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Polls MySQL every 30 seconds for config changes.
 * Worker-side: READ-ONLY access to topic_sink_config.
 *
 * Handles:
 *   - Added topics   → VersionedConfigSync schedules subscription update
 *   - Removed topics → flush buffer, unsubscribe
 *   - Schema changed → flush old buffer, rebuild with new schema
 *   - Other changed  → update flush thresholds or flush+rebuild for path/format changes
 */
@Slf4j
@Component
public class ConfigPoller {

    private volatile Map<String, TopicSinkConfig> currentConfigMap = new ConcurrentHashMap<>();

    private final TopicSinkConfigRepository configRepo;
    private final TopicMatcher topicMatcher;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final NodeIdentity nodeIdentity;
    private final VersionedConfigSync versionedSync;

    public ConfigPoller(TopicSinkConfigRepository configRepo,
                        TopicMatcher topicMatcher,
                        WriteBufferManager bufferManager,
                        FlushExecutor flushExecutor,
                        NodeIdentity nodeIdentity) {
        this.configRepo = configRepo;
        this.topicMatcher = topicMatcher;
        this.bufferManager = bufferManager;
        this.flushExecutor = flushExecutor;
        this.nodeIdentity  = nodeIdentity;
        this.versionedSync = new VersionedConfigSync(this::applySubscriptionChange);
    }

    @PostConstruct
    public void init() {
        syncConfig();
    }

    /** Poll every 30 seconds */
    @Scheduled(fixedDelayString = "${lake-writer.config.sync-interval-sec:30}000")
    public void syncConfig() {
        try {
            List<TopicSinkConfig> active = configRepo.findAllActive();
            Map<String, TopicSinkConfig> latestMap = active.stream()
                .collect(Collectors.toMap(TopicSinkConfig::getTopicName, c -> c));

            ConfigDiff diff = ConfigDiff.compute(currentConfigMap, latestMap);
            if (diff.isEmpty()) return;

            log.info("[ConfigSync] Changes detected: added={}, removed={}, schemaChanged={}, otherChanged={}",
                diff.getAdded().size(), diff.getRemoved().size(),
                diff.getSchemaChanged().size(), diff.getOtherChanged().size());

            // Reload topic matcher with new full config
            topicMatcher.reload(new ArrayList<>(latestMap.values()));

            // Handle each change type
            diff.getAdded().forEach(c -> versionedSync.onConfigChanged(c));
            diff.getRemoved().forEach(c -> handleTopicRemoved(c));
            diff.getSchemaChanged().forEach(c -> handleSchemaChanged(currentConfigMap.get(c.getTopicName()), c));
            diff.getOtherChanged().forEach(c -> handleOtherChanged(currentConfigMap.get(c.getTopicName()), c));

            currentConfigMap = new ConcurrentHashMap<>(latestMap);

        } catch (Exception e) {
            log.error("[ConfigSync] Sync failed, keeping current config: {}", e.getMessage(), e);
        }
    }

    public Map<String, TopicSinkConfig> getCurrentConfigMap() {
        return currentConfigMap;
    }

    private void applySubscriptionChange(TopicSinkConfig config) {
        // KafkaConsumerPool.subscribe() would be called here via callback
        // For now, logged — the consumer pool should be wired in at startup
        log.info("[ConfigSync] Subscription change applied: topic={}", config.getTopicName());
    }

    private void handleTopicRemoved(TopicSinkConfig config) {
        List<DoubleWriteBuffer> bufs = bufferManager.getBuffersByTopic(config.getTopicName());
        for (DoubleWriteBuffer dbuf : bufs) {
            WriteBuffer toFlush = dbuf.swapForFlush();
            if (!toFlush.isEmpty()) {
                flushExecutor.flush(toFlush, config.parseSchema(), nodeIdentity.getNodeId());
            }
            bufferManager.removeBuffer(toFlush.getTopicPartition());
        }
        log.info("[ConfigSync] Topic removed and buffers flushed: {}", config.getTopicName());
    }

    private void handleSchemaChanged(TopicSinkConfig oldConfig, TopicSinkConfig newConfig) {
        List<DoubleWriteBuffer> bufs = bufferManager.getBuffersByTopic(newConfig.getTopicName());
        for (DoubleWriteBuffer dbuf : bufs) {
            WriteBuffer toFlush = dbuf.swapForFlush();
            if (!toFlush.isEmpty()) {
                // Flush with OLD schema
                flushExecutor.flush(toFlush, oldConfig.parseSchema(), nodeIdentity.getNodeId());
            }
            // Recreate buffer with NEW config
            bufferManager.createBuffer(toFlush.getTopicPartition(), newConfig);
        }
        log.info("[ConfigSync] Schema changed, buffers rebuilt: topic={}", newConfig.getTopicName());
    }

    private void handleOtherChanged(TopicSinkConfig oldConfig, TopicSinkConfig newConfig) {
        // Always flush existing buffer data before applying the new config.
        // Even a flush-threshold-only change must not silently discard in-memory rows:
        // if we replaced the DoubleWriteBuffer object directly, any accumulated data in the
        // old buffer would be lost (no offset committed, no file written).
        // Reusing handleSchemaChanged() — flush with current schema, recreate with new config —
        // is safe for all change types and eliminates the data-loss path.
        handleSchemaChanged(oldConfig, newConfig);
        log.info("[ConfigSync] Config updated (flush+rebuild): topic={}", newConfig.getTopicName());
    }

    private static boolean safeEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
}
