package com.lakewriter.worker.checkpoint;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Records the state of a single in-flight flush.
 * Written to local disk at Phase 2 of 5-phase commit.
 * Used during crash recovery to determine what to do with in-progress files.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Checkpoint {

    private String topic;
    private int partition;
    private long startOffset;
    private long endOffset;
    private int recordCount;
    private String tmpFilePath;
    private String targetFilePath;
    private String nodeId;
    private String createdAt;

    public Checkpoint(String topic, int partition, long startOffset, long endOffset,
                      int recordCount, String tmpFilePath, String targetFilePath, String nodeId) {
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.recordCount = recordCount;
        this.tmpFilePath = tmpFilePath;
        this.targetFilePath = targetFilePath;
        this.nodeId = nodeId;
        this.createdAt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
    }

    public String toJson() {
        return JSON.toJSONString(this);
    }

    public static Checkpoint fromJson(String json) {
        return JSON.parseObject(json, Checkpoint.class);
    }
}
