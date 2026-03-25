package com.lakewriter.admin.service;

import com.lakewriter.common.model.WorkerHeartbeat;
import com.lakewriter.common.repository.WorkerHeartbeatRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Aggregates Worker node status from the worker_heartbeat table.
 * Workers are considered OFFLINE if last_heartbeat > 60s ago.
 */
@Service
public class StatusAggregator {

    private static final long OFFLINE_THRESHOLD_MS = 60_000L;

    private final WorkerHeartbeatRepository heartbeatRepo;

    public StatusAggregator(WorkerHeartbeatRepository heartbeatRepo) {
        this.heartbeatRepo = heartbeatRepo;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> aggregateAll() {
        List<WorkerHeartbeat> heartbeats = heartbeatRepo.findAll();
        Date threshold = new Date(System.currentTimeMillis() - OFFLINE_THRESHOLD_MS);

        List<Map<String, Object>> result = new ArrayList<>();
        for (WorkerHeartbeat hb : heartbeats) {
            Map<String, Object> ws = new HashMap<>();
            boolean online = hb.getLastHeartbeat() != null && hb.getLastHeartbeat().after(threshold);
            ws.put("nodeId", hb.getNodeId());
            ws.put("online", online);
            ws.put("status", online ? "ONLINE" : "OFFLINE");
            ws.put("recordsPerSec", hb.getRecordsPerSec());
            ws.put("bufferUsageBytes", hb.getBufferUsageBytes());
            ws.put("bufferUsageMb", Math.round(hb.getBufferUsageBytes() / (1024.0 * 1024.0) * 10) / 10.0);
            ws.put("uptimeSec", hb.getUptimeSec());
            ws.put("lastHeartbeat", hb.getLastHeartbeat());
            ws.put("assignedPartitions", hb.getAssignedPartitions());
            result.add(ws);
        }
        return result;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> getSummary() {
        List<Map<String, Object>> nodes = aggregateAll();
        long online = nodes.stream().filter(n -> Boolean.TRUE.equals(n.get("online"))).count();
        double totalRps = nodes.stream().mapToDouble(n -> (Double) n.getOrDefault("recordsPerSec", 0.0)).sum();

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalNodes", nodes.size());
        summary.put("onlineNodes", online);
        summary.put("totalRecordsPerSec", totalRps);
        summary.put("nodes", nodes);
        return summary;
    }
}
