package com.lakewriter.common.model;

import com.alibaba.fastjson2.JSON;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * JPA entity for topic_sink_config table.
 * Holds all configuration for writing a Kafka topic to HDFS/OSS.
 */
@Data
@Entity
@Table(name = "topic_sink_config")
public class TopicSinkConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Kafka topic name; may be a regex pattern when matchType=REGEX */
    @Column(name = "topic_name", nullable = false, unique = true)
    private String topicName;

    /** EXACT or REGEX */
    @Column(name = "match_type", nullable = false)
    private String matchType = "EXACT";

    /** JSON schema: {"fields":[{"name":"id","type":"LONG","nullable":false},...]} */
    @Column(name = "schema_json", nullable = false, columnDefinition = "TEXT")
    private String schemaJson;

    /** PARQUET or CSV */
    @Column(name = "sink_format", nullable = false)
    private String sinkFormat = "PARQUET";

    /** Path template: /hdfs/data/{topic}/{date} */
    @Column(name = "sink_path", nullable = false)
    private String sinkPath;

    /** Comma-separated partition fields, e.g. "dt,hour" */
    @Column(name = "partition_by")
    private String partitionBy;

    /** SNAPPY | GZIP | NONE */
    @Column(name = "compression", nullable = false)
    private String compression = "SNAPPY";

    @Column(name = "flush_rows", nullable = false)
    private int flushRows = 500000;

    @Column(name = "flush_bytes", nullable = false)
    private long flushBytes = 268435456L;

    @Column(name = "flush_interval_sec", nullable = false)
    private int flushIntervalSec = 600;

    /** ACTIVE or PAUSED */
    @Column(name = "status", nullable = false)
    private String status = "ACTIVE";

    /** Optimistic lock version — incremented on each update so Workers detect changes */
    @Column(name = "version", nullable = false)
    private int version = 1;

    @Column(name = "description")
    private String description;

    @Column(name = "created_at")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt;

    @Column(name = "updated_at")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedAt;

    /** Parse schemaJson into a SchemaDefinition object */
    @Transient
    public SchemaDefinition parseSchema() {
        return JSON.parseObject(this.schemaJson, SchemaDefinition.class);
    }

    /** Convenience: get fields as array */
    @Transient
    public FieldDef[] getFieldArray() {
        SchemaDefinition schema = parseSchema();
        return schema.getFields().toArray(new FieldDef[0]);
    }
}
