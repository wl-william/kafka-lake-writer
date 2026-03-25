package com.lakewriter.worker.consumer;

import com.lakewriter.common.model.FieldDef;
import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.buffer.DoubleWriteBuffer;
import com.lakewriter.worker.buffer.WriteBufferManager;
import com.lakewriter.worker.config.TopicMatcher;
import com.lakewriter.worker.schema.JsonRecordParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Routes each Kafka record to the appropriate WriteBuffer.
 * Handles JSON parsing and schema mapping inline.
 */
@Slf4j
public class RecordDispatcher {

    private final WriteBufferManager bufferManager;
    private final TopicMatcher topicMatcher;
    private final JsonRecordParser parser;

    public RecordDispatcher(WriteBufferManager bufferManager, TopicMatcher topicMatcher,
                            JsonRecordParser parser) {
        this.bufferManager = bufferManager;
        this.topicMatcher  = topicMatcher;
        this.parser = parser;
    }

    public void dispatch(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        TopicSinkConfig config = topicMatcher.match(topic);
        if (config == null) {
            log.debug("No config for topic {}, skipping record", topic);
            return;
        }

        TopicPartition tp = new TopicPartition(topic, record.partition());
        DoubleWriteBuffer buf = bufferManager.getBuffer(tp);
        if (buf == null) {
            // Buffer may not exist yet if partition was just assigned — create lazily
            bufferManager.createBuffer(tp, config);
            buf = bufferManager.getBuffer(tp);
        }

        FieldDef[] schema = config.getFieldArray();
        Object[] row = parser.parseToRow(record.value(), schema);
        if (row == null) return;   // parse error — already logged/metered

        // Records polled from Kafka MUST be appended to the buffer (At-Least-Once guarantee).
        // Backpressure is enforced at the poll() level in ConsumerWorker, not here.
        long estimatedBytes = record.serializedValueSize();
        bufferManager.getBackpressure().add(estimatedBytes);
        buf.append(row, record.offset(), estimatedBytes);
    }
}
