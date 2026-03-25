package com.lakewriter.worker.schema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Resolves path templates like "/hdfs/data/{topic}/{date}" to actual paths.
 *
 * Supported variables:
 *   {topic}    - Kafka topic name
 *   {date}     - system date yyyy-MM-dd (auto daily directory)
 *   {dt}       - message field "dt", falls back to system date
 *   {hour}     - message field "hour", falls back to current hour (HH)
 *   {minute}   - message field "minute", falls back to current minute
 *   {partition}- Kafka partition number (passed explicitly)
 */
public class PathResolver {

    private PathResolver() {}

    /**
     * Resolve template using system date/time and optional message fields.
     *
     * @param template    e.g. "/hdfs/kafka/data/{topic}/{date}"
     * @param topic       Kafka topic name
     * @param msgFields   extracted fields from the Kafka message (may be null)
     */
    public static String resolve(String template, String topic, Map<String, String> msgFields) {
        if (template == null) return "";

        String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String currentHour = new SimpleDateFormat("HH").format(new Date());
        String currentMinute = new SimpleDateFormat("mm").format(new Date());

        String result = template;
        result = result.replace("{topic}", topic != null ? topic : "");
        result = result.replace("{date}", today);

        if (msgFields != null) {
            result = result.replace("{dt}", msgFields.getOrDefault("dt", today));
            result = result.replace("{hour}", msgFields.getOrDefault("hour", currentHour));
            result = result.replace("{minute}", msgFields.getOrDefault("minute", currentMinute));
        } else {
            result = result.replace("{dt}", today);
            result = result.replace("{hour}", currentHour);
            result = result.replace("{minute}", currentMinute);
        }
        return result;
    }

    /**
     * Preview resolution using only system date/time — no message fields.
     * Used by Admin UI PathPreview component.
     */
    public static String preview(String template, String topic) {
        return resolve(template, topic, null);
    }

    /**
     * Resolve with partition variable.
     */
    public static String resolveWithPartition(String template, String topic,
                                               Map<String, String> msgFields, int partition) {
        String path = resolve(template, topic, msgFields);
        return path.replace("{partition}", String.valueOf(partition));
    }
}
