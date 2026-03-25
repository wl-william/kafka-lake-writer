package com.lakewriter.worker.node;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Generates a unique node ID for this Worker instance.
 * Priority: POD_NAME (K8s) > HOSTNAME (Docker) > NODE_ID (manual) > hostname > random
 *
 * The nodeId is embedded in every output filename to prevent multi-node conflicts:
 *   part-{nodeId}-P{partition}-{start}to{end}-{ts}.snappy.parquet
 */
@Slf4j
public class NodeIdentity {

    private final String nodeId;

    public NodeIdentity() {
        this.nodeId = resolveNodeId();
        log.info("Node identity: {}", nodeId);
    }

    public NodeIdentity(String configuredId) {
        this.nodeId = (configuredId != null && !configuredId.isEmpty())
            ? configuredId : resolveNodeId();
        log.info("Node identity: {}", nodeId);
    }

    public String getNodeId() {
        return nodeId;
    }

    private String resolveNodeId() {
        // K8s Pod name
        String id = System.getenv("POD_NAME");
        if (isValid(id)) return sanitize(id);

        // Docker container hostname
        id = System.getenv("HOSTNAME");
        if (isValid(id)) return sanitize(id);

        // Explicit env override
        id = System.getenv("NODE_ID");
        if (isValid(id)) return sanitize(id);

        // OS hostname
        try {
            id = InetAddress.getLocalHost().getHostName();
            if (isValid(id)) return sanitize(id);
        } catch (Exception ignored) {}

        // Fallback: random suffix
        return "node-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private boolean isValid(String s) {
        return s != null && !s.trim().isEmpty();
    }

    /** Replace characters invalid in filenames */
    private String sanitize(String s) {
        return s.replaceAll("[^a-zA-Z0-9._-]", "-").toLowerCase();
    }
}
