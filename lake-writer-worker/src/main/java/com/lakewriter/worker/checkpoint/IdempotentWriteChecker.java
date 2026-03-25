package com.lakewriter.worker.checkpoint;

import com.lakewriter.worker.storage.StorageAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Prevents duplicate file writes in crash-recovery and multi-node scenarios.
 *
 * Builds an index of already-written offset ranges by scanning existing filenames.
 * File name format: part-{nodeId}-P{partition}-{startOffset}to{endOffset}-{ts}.{ext}
 *
 * Usage: call buildIndex() once at startup, then isAlreadyWritten() before each flush.
 */
@Slf4j
public class IdempotentWriteChecker {

    // Pattern matches: part-<nodeId>-P<partition>-<start>to<end>-<ts>.<ext>
    private static final Pattern FILE_PATTERN = Pattern.compile(
        "part-[^-]+-P(\\d+)-(\\d+)to(\\d+)-.*"
    );

    // key: TopicPartition, value: TreeMap<startOffset, endOffset>
    private final Map<TopicPartition, TreeMap<Long, Long>> writtenRanges = new ConcurrentHashMap<>();
    private final StorageAdapter storage;

    public IdempotentWriteChecker(StorageAdapter storage) {
        this.storage = storage;
    }

    /**
     * Scan all files in sinkPath and build the offset-range index.
     * Call once per topic+path at startup.
     */
    public void buildIndex(String sinkPath, String topic) {
        try {
            List<String> files = storage.listFiles(sinkPath);
            int indexed = 0;
            for (String filePath : files) {
                String name = filePath.substring(filePath.lastIndexOf('/') + 1);
                Matcher m = FILE_PATTERN.matcher(name);
                if (m.matches()) {
                    int partition   = Integer.parseInt(m.group(1));
                    long startOffset = Long.parseLong(m.group(2));
                    long endOffset   = Long.parseLong(m.group(3));
                    TopicPartition tp = new TopicPartition(topic, partition);
                    writtenRanges.computeIfAbsent(tp, k -> new TreeMap<>())
                                 .put(startOffset, endOffset);
                    indexed++;
                }
            }
            log.info("[IdempotentCheck] Indexed {} files in {}", indexed, sinkPath);
        } catch (IOException e) {
            log.warn("[IdempotentCheck] Failed to build index for path={}, topic={}", sinkPath, topic, e);
        }
    }

    /**
     * Returns true if the range [startOffset, endOffset] is fully covered by an existing file.
     * Prevents re-writing data that was already successfully flushed.
     */
    public boolean isAlreadyWritten(TopicPartition tp, long startOffset, long endOffset) {
        TreeMap<Long, Long> ranges = writtenRanges.get(tp);
        if (ranges == null) return false;
        Map.Entry<Long, Long> floor = ranges.floorEntry(startOffset);
        return floor != null && floor.getValue() >= endOffset;
    }

    /** Record a newly completed write so subsequent checks detect it */
    public void recordWritten(TopicPartition tp, long startOffset, long endOffset) {
        writtenRanges.computeIfAbsent(tp, k -> new TreeMap<>()).put(startOffset, endOffset);
    }
}
