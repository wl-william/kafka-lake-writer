package com.lakewriter.common.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * Audit log entry recording every config change.
 * Written by Admin service, readable via /api/v1/configs/{id}/changelog.
 */
@Data
@Entity
@Table(name = "config_change_log")
public class ConfigChangeLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "config_id", nullable = false)
    private Long configId;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    /** CREATED | UPDATED | DELETED | PAUSED | RESUMED */
    @Column(name = "change_type", nullable = false)
    private String changeType;

    @Column(name = "operator")
    private String operator;

    @Column(name = "old_value", columnDefinition = "TEXT")
    private String beforeJson;

    @Column(name = "new_value", columnDefinition = "TEXT")
    private String afterJson;

    @Column(name = "created_at")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt;
}
