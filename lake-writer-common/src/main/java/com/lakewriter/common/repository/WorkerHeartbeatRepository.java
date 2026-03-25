package com.lakewriter.common.repository;

import com.lakewriter.common.model.WorkerHeartbeat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

@Repository
public interface WorkerHeartbeatRepository extends JpaRepository<WorkerHeartbeat, String> {

    /**
     * Upsert worker heartbeat using JPQL merge-style query.
     * Spring Data JPA save() handles INSERT vs UPDATE by PK.
     * This custom query does a MySQL INSERT ... ON DUPLICATE KEY UPDATE.
     */
    @Modifying
    @Transactional
    @Query(value = "INSERT INTO worker_heartbeat " +
        "(node_id, status, assigned_partitions, consumer_status, records_per_sec, buffer_usage_bytes, uptime_sec, last_heartbeat) " +
        "VALUES (:#{#hb.nodeId}, :#{#hb.status}, :#{#hb.assignedPartitions}, :#{#hb.consumerStatus}, " +
        ":#{#hb.recordsPerSec}, :#{#hb.bufferUsageBytes}, :#{#hb.uptimeSec}, :#{#hb.lastHeartbeat}) " +
        "ON DUPLICATE KEY UPDATE status=VALUES(status), assigned_partitions=VALUES(assigned_partitions), " +
        "consumer_status=VALUES(consumer_status), records_per_sec=VALUES(records_per_sec), " +
        "buffer_usage_bytes=VALUES(buffer_usage_bytes), uptime_sec=VALUES(uptime_sec), " +
        "last_heartbeat=VALUES(last_heartbeat)",
        nativeQuery = true)
    void upsert(@Param("hb") WorkerHeartbeat hb);

    /** Mark nodes with stale heartbeats as OFFLINE */
    @Modifying
    @Transactional
    @Query("UPDATE WorkerHeartbeat w SET w.status = 'OFFLINE' WHERE w.lastHeartbeat < :threshold")
    int markOfflineByThreshold(@Param("threshold") Date threshold);
}
