package com.lakewriter.worker.heartbeat;

import com.alibaba.fastjson2.JSON;
import com.lakewriter.common.model.WorkerHeartbeat;
import com.lakewriter.common.repository.WorkerHeartbeatRepository;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.consumer.KafkaConsumerPool;
import com.lakewriter.worker.node.NodeIdentity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reports Worker status to MySQL every 15 seconds.
 * Admin reads this table to show node health and partition assignments.
 *
 * Intentionally lightweight — doesn't call Admin, only writes MySQL.
 */
@Slf4j
@Component
public class HeartbeatReporter {

    private final WorkerHeartbeatRepository heartbeatRepo;
    private final WriteBufferManager bufferManager;
    private final NodeIdentity nodeIdentity;
    private final long startTimeMs = System.currentTimeMillis();

    // KafkaConsumerPool may be null if startup not complete yet
    private KafkaConsumerPool consumerPool;

    public HeartbeatReporter(WorkerHeartbeatRepository heartbeatRepo,
                              WriteBufferManager bufferManager,
                              NodeIdentity nodeIdentity) {
        this.heartbeatRepo = heartbeatRepo;
        this.bufferManager = bufferManager;
        this.nodeIdentity  = nodeIdentity;
    }

    public void setConsumerPool(KafkaConsumerPool consumerPool) {
        this.consumerPool = consumerPool;
    }

    @Scheduled(fixedDelayString = "${lake-writer.heartbeat.interval-sec:15}000")
    public void report() {
        try {
            WorkerHeartbeat hb = new WorkerHeartbeat();
            hb.setNodeId(nodeIdentity.getNodeId());
            hb.setStatus("ONLINE");
            hb.setBufferUsageBytes(bufferManager.getTotalBufferBytes());
            hb.setUptimeSec((System.currentTimeMillis() - startTimeMs) / 1000);
            hb.setLastHeartbeat(new Date());

            if (consumerPool != null) {
                Set<org.apache.kafka.common.TopicPartition> assigned = consumerPool.getAssignedPartitions();
                // Serialize as {"topic":[partition,...]}
                Map<String, java.util.List<Integer>> partMap = new HashMap<>();
                for (org.apache.kafka.common.TopicPartition tp : assigned) {
                    partMap.computeIfAbsent(tp.topic(), k -> new java.util.ArrayList<>()).add(tp.partition());
                }
                hb.setAssignedPartitions(JSON.toJSONString(partMap));
            }

            heartbeatRepo.upsert(hb);
            log.debug("Heartbeat reported: node={}, bufferBytes={}", hb.getNodeId(), hb.getBufferUsageBytes());
        } catch (Exception e) {
            log.warn("Failed to report heartbeat: {}", e.getMessage());
        }
    }
}
