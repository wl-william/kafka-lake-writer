package com.lakewriter.worker.schema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Resolves path templates like "/hdfs/data/{topic}/{date}" to actual paths.
 *
 * Supported variables:
 *   {topic}    - Kafka topic name
 *   {date}     - date from the record timestamp (yyyy-MM-dd), NOT system time
 *   {dt}       - message field "dt", falls back to record date
 *   {hour}     - message field "hour", falls back to record hour (HH)
 *   {minute}   - message field "minute", falls back to record minute
 *   {partition}- Kafka partition number (passed explicitly)
 *
 * Security: all substitution values are sanitized to [a-zA-Z0-9_.\-] to prevent
 * path traversal attacks. The final resolved path is validated to not contain ".." segments.
 */
public class PathResolver {

    /** Only allow safe characters in substitution values */
    private static final Pattern SAFE_VALUE = Pattern.compile("[a-zA-Z0-9_.\\-]+");

    private PathResolver() {}

    /**
     * Resolve template using the Kafka record timestamp for date/time variables.
     *
     * @param template       e.g. "/hdfs/kafka/data/{topic}/{date}"
     * @param topic          Kafka topic name
     * @param timestampMs    epoch millis — typically the first record's Kafka timestamp
     * @param msgFields      extracted fields from the Kafka message (may be null)
     * @throws IllegalArgumentException if the resolved path contains ".." (path traversal)
     */
    public static String resolve(String template, String topic, long timestampMs,
                                  Map<String, String> msgFields) {
        if (template == null) return "";

        Date recordDate = (timestampMs > 0) ? new Date(timestampMs) : new Date();
        String dateStr   = new SimpleDateFormat("yyyy-MM-dd").format(recordDate);
        String hourStr   = new SimpleDateFormat("HH").format(recordDate);
        String minuteStr = new SimpleDateFormat("mm").format(recordDate);

        String result = template;
        result = result.replace("{topic}", sanitize(topic != null ? topic : "", "topic"));
        result = result.replace("{date}", dateStr);

        if (msgFields != null) {
            result = result.replace("{dt}", sanitize(msgFields.getOrDefault("dt", dateStr), "dt"));
            result = result.replace("{hour}", sanitize(msgFields.getOrDefault("hour", hourStr), "hour"));
            result = result.replace("{minute}", sanitize(msgFields.getOrDefault("minute", minuteStr), "minute"));
        } else {
            result = result.replace("{dt}", dateStr);
            result = result.replace("{hour}", hourStr);
            result = result.replace("{minute}", minuteStr);
        }

        validateNoTraversal(result);
        return result;
    }

    /**
     * Preview resolution using current system time — no message fields.
     * Used by Admin UI PathPreview component.
     */
    public static String preview(String template, String topic) {
        return resolve(template, topic, System.currentTimeMillis(), null);
    }

    /**
     * Resolve with partition variable.
     */
    public static String resolveWithPartition(String template, String topic,
                                               long timestampMs,
                                               Map<String, String> msgFields, int partition) {
        String path = resolve(template, topic, timestampMs, msgFields);
        return path.replace("{partition}", String.valueOf(partition));
    }

    /**
     * Sanitize a substitution value to prevent path traversal.
     * Only allows [a-zA-Z0-9_.\-]. Unsafe characters are replaced with '_'.
     */
    private static String sanitize(String value, String fieldName) {
        if (value == null || value.isEmpty()) return "";
        if (SAFE_VALUE.matcher(value).matches()) return value;
        // Replace unsafe characters
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '.' || c == '-') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    /**
     * Reject resolved paths containing ".." to prevent directory traversal.
     */
    private static void validateNoTraversal(String path) {
        if (path.contains("..")) {
            throw new IllegalArgumentException(
                "Resolved path contains '..' (path traversal rejected): " + path);
        }
    }
}
