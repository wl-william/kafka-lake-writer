package com.lakewriter.worker.config;

import com.lakewriter.common.model.TopicSinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Matches a Kafka topic name to its TopicSinkConfig.
 * EXACT match has priority over REGEX.
 * Among REGEX patterns, first match in insertion order wins.
 */
@Slf4j
public class TopicMatcher {

    private volatile Map<String, TopicSinkConfig> exactConfigs = new HashMap<>();
    private volatile Map<Pattern, TopicSinkConfig> regexConfigs = new LinkedHashMap<>();

    /** Reload all configs — called when ConfigPoller detects a change */
    public synchronized void reload(List<TopicSinkConfig> configs) {
        Map<String, TopicSinkConfig> exact = new HashMap<>();
        Map<Pattern, TopicSinkConfig> regex = new LinkedHashMap<>();

        for (TopicSinkConfig config : configs) {
            if ("EXACT".equals(config.getMatchType())) {
                exact.put(config.getTopicName(), config);
            } else if ("REGEX".equals(config.getMatchType())) {
                try {
                    regex.put(Pattern.compile(config.getTopicName()), config);
                } catch (Exception e) {
                    log.warn("Invalid regex pattern '{}': {}", config.getTopicName(), e.getMessage());
                }
            }
        }

        this.exactConfigs = exact;
        this.regexConfigs = regex;
        log.info("TopicMatcher reloaded: exact={}, regex={}", exact.size(), regex.size());
    }

    /**
     * Find the TopicSinkConfig for a topic name.
     * Returns null if no config matches (topic should be ignored).
     * Regex matching is protected against catastrophic backtracking — a failed
     * match is cached per-pattern and logged once.
     */
    public TopicSinkConfig match(String topic) {
        // Exact match first
        TopicSinkConfig cfg = exactConfigs.get(topic);
        if (cfg != null) return cfg;

        // Regex match (first wins)
        for (Map.Entry<Pattern, TopicSinkConfig> entry : regexConfigs.entrySet()) {
            try {
                if (entry.getKey().matcher(topic).matches()) {
                    return entry.getValue();
                }
            } catch (Exception e) {
                log.warn("Regex match error for pattern '{}' on topic '{}': {}",
                    entry.getKey().pattern(), topic, e.getMessage());
            }
        }
        return null;
    }

    /**
     * Resolve the full set of topics to subscribe to.
     * Regex configs are resolved against the provided list of available Kafka topics.
     */
    public Set<String> resolveSubscriptionTopics(Set<String> allKafkaTopics) {
        Set<String> result = new HashSet<>(exactConfigs.keySet());
        for (Map.Entry<Pattern, TopicSinkConfig> entry : regexConfigs.entrySet()) {
            for (String topic : allKafkaTopics) {
                if (entry.getKey().matcher(topic).matches()) {
                    result.add(topic);
                }
            }
        }
        return result;
    }

    public Set<String> getExactTopics() {
        return new HashSet<>(exactConfigs.keySet());
    }
}
