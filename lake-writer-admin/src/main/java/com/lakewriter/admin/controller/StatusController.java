package com.lakewriter.admin.controller;

import com.lakewriter.admin.service.StatusAggregator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST API for Worker node status (read from worker_heartbeat table).
 *
 * GET /api/v1/status/nodes   - all Worker nodes with online/offline status
 * GET /api/v1/status/summary - aggregate summary (total nodes, RPS)
 */
@RestController
@RequestMapping("/api/v1/status")
public class StatusController {

    private final StatusAggregator aggregator;

    public StatusController(StatusAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @GetMapping("/nodes")
    public ResponseEntity<?> nodes() {
        return ok(aggregator.aggregateAll());
    }

    @GetMapping("/summary")
    public ResponseEntity<?> summary() {
        return ok(aggregator.getSummary());
    }

    private ResponseEntity<?> ok(Object data) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 200);
        r.put("data", data);
        return ResponseEntity.ok(r);
    }
}
