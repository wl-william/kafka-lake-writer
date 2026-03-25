package com.lakewriter.worker.lifecycle;

import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.consumer.KafkaConsumerPool;
import com.lakewriter.worker.heartbeat.HeartbeatReporter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Starts the KafkaConsumerPool after the Spring context is fully initialized.
 * This ensures ConfigPoller.init() has already loaded TopicMatcher before
 * consumers start polling.
 */
@Slf4j
@Component
public class WorkerStartupRunner implements ApplicationRunner {

    private final KafkaConsumerPool consumerPool;
    private final TopicMatcher topicMatcher;
    private final HeartbeatReporter heartbeatReporter;

    public WorkerStartupRunner(KafkaConsumerPool consumerPool,
                                TopicMatcher topicMatcher,
                                HeartbeatReporter heartbeatReporter) {
        this.consumerPool      = consumerPool;
        this.topicMatcher      = topicMatcher;
        this.heartbeatReporter = heartbeatReporter;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Wire consumer pool into heartbeat reporter (circular dependency avoided via setter)
        heartbeatReporter.setConsumerPool(consumerPool);

        // Start consuming — subscribe to all exact topics known at startup
        // Regex-matched topics will be resolved when Kafka sends topic metadata
        java.util.Set<String> topics = topicMatcher.getExactTopics();
        if (topics.isEmpty()) {
            log.warn("[Startup] No topics configured yet — consumer will start after first ConfigPoller sync");
            // Subscribe to wildcard pattern — Kafka will match when topics are created
            topics = java.util.Collections.singleton(".*");
        }

        log.info("[Startup] Starting consumer pool for topics: {}", topics);
        consumerPool.start(topics);
        log.info("[Startup] Worker is running");
    }
}
