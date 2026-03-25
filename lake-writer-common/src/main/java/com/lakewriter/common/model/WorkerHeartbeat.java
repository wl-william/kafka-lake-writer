package com.lakewriter.common.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * Worker heartbeat — written by Worker every 15s, read by Admin to show node status.
 * Uses node_id as PK so each worker upserts its own row.
 */
@Data
@Entity
@Table(name = "worker_heartbeat")
public class WorkerHeartbeat {

    /** e.g. pod-01 or hostname */
    @Id
    @Column(name = "node_id", nullable = false)
    private String nodeId;

    /** ONLINE | OFFLINE */
    @Column(name = "status", nullable = false)
    private String status = "ONLINE";

    /** JSON: {"orders":[0,1,2],"logs":[3,4]} */
    @Column(name = "assigned_partitions", columnDefinition = "TEXT")
    private String assignedPartitions;

    /** JSON: per-partition offset/lag/buffer info */
    @Column(name = "consumer_status", columnDefinition = "TEXT")
    private String consumerStatus;

    @Column(name = "records_per_sec", nullable = false)
    private double recordsPerSec;

    @Column(name = "buffer_usage_bytes", nullable = false)
    private long bufferUsageBytes;

    @Column(name = "uptime_sec", nullable = false)
    private long uptimeSec;

    @Column(name = "last_heartbeat")
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastHeartbeat;
}
