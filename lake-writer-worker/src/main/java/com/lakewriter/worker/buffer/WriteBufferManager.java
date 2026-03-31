package com.lakewriter.worker.buffer;

import com.lakewriter.common.model.TopicSinkConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages all DoubleWriteBuffers — one per TopicPartition.
 * Lifecycle methods called by SafeRebalanceListener and ConfigPoller.
 */
@Slf4j
public class WriteBufferManager {

    private final ConcurrentHashMap<TopicPartition, DoubleWriteBuffer> buffers = new ConcurrentHashMap<>();

    @Getter
    private final BackpressureController backpressure;

    public WriteBufferManager(long maxTotalBytes) {
        this.backpressure = new BackpressureController(maxTotalBytes);
    }

    public void createBuffer(TopicPartition tp, TopicSinkConfig config) {
        buffers.put(tp, new DoubleWriteBuffer(tp, config));
        log.info("Created buffer for {}", tp);
    }

    public DoubleWriteBuffer getBuffer(TopicPartition tp) {
        return buffers.get(tp);
    }

    public void removeBuffer(TopicPartition tp) {
        buffers.remove(tp);
        log.info("Removed buffer for {}", tp);
    }

    /** Get all buffers for a given topic (across all partitions) */
    public List<DoubleWriteBuffer> getBuffersByTopic(String topic) {
        List<DoubleWriteBuffer> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, DoubleWriteBuffer> entry : buffers.entrySet()) {
            if (entry.getKey().topic().equals(topic)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    /** Get all buffers (used by graceful shutdown) */
    public List<DoubleWriteBuffer> getAllBuffers() {
        return new ArrayList<>(buffers.values());
    }

    public long getTotalBufferBytes() {
        long total = 0;
        for (DoubleWriteBuffer buf : buffers.values()) {
            total += buf.getActiveBuffer().getTotalBytes();
        }
        return total;
    }

}
