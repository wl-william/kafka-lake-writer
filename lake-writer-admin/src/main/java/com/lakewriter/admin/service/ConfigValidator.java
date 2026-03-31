package com.lakewriter.admin.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates TopicSinkConfig before saving.
 * Returns a list of error messages (empty = valid).
 */
@Service
public class ConfigValidator {

    private static final List<String> VALID_TYPES =
        Arrays.asList("STRING", "INT", "LONG", "FLOAT", "DOUBLE", "BOOLEAN");

    private static final List<String> VALID_FORMATS = Arrays.asList("PARQUET", "CSV");
    private static final List<String> VALID_COMPRESSIONS = Arrays.asList("SNAPPY", "GZIP", "NONE");

    /** Only allow safe characters in EXACT topic names */
    private static final Pattern SAFE_TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._\\-]+");

    /** Max length for string fields to prevent abuse */
    private static final int MAX_TOPIC_NAME_LEN = 255;
    private static final int MAX_SINK_PATH_LEN = 512;
    private static final int MAX_SCHEMA_JSON_LEN = 32768;  // 32KB
    private static final int MAX_DESCRIPTION_LEN = 512;

    /** Max regex pattern length to limit ReDoS risk */
    private static final int MAX_REGEX_LEN = 128;

    /** Patterns known to cause catastrophic backtracking */
    private static final Pattern DANGEROUS_REGEX = Pattern.compile(
        ".*\\(([^)]*[+*]){2,}.*"  // nested quantifiers like (a+)+ or (a*)*
    );

    public List<String> validate(TopicSinkConfig config) {
        List<String> errors = new ArrayList<>();

        // topicName validation
        if (config.getTopicName() == null || config.getTopicName().trim().isEmpty()) {
            errors.add("topicName is required");
        } else if (config.getTopicName().length() > MAX_TOPIC_NAME_LEN) {
            errors.add("topicName exceeds max length " + MAX_TOPIC_NAME_LEN);
        } else if ("EXACT".equals(config.getMatchType())) {
            if (!SAFE_TOPIC_NAME.matcher(config.getTopicName()).matches()) {
                errors.add("topicName (EXACT) may only contain [a-zA-Z0-9._-]");
            }
        } else if ("REGEX".equals(config.getMatchType())) {
            errors.addAll(validateRegex(config.getTopicName()));
        }

        // sinkPath validation
        if (config.getSinkPath() == null || config.getSinkPath().trim().isEmpty()) {
            errors.add("sinkPath is required");
        } else if (config.getSinkPath().length() > MAX_SINK_PATH_LEN) {
            errors.add("sinkPath exceeds max length " + MAX_SINK_PATH_LEN);
        } else if (config.getSinkPath().contains("..")) {
            errors.add("sinkPath must not contain '..' (path traversal)");
        }

        // description length
        if (config.getDescription() != null && config.getDescription().length() > MAX_DESCRIPTION_LEN) {
            errors.add("description exceeds max length " + MAX_DESCRIPTION_LEN);
        }

        if (!VALID_FORMATS.contains(config.getSinkFormat())) {
            errors.add("sinkFormat must be one of: " + VALID_FORMATS);
        }
        if (!VALID_COMPRESSIONS.contains(config.getCompression())) {
            errors.add("compression must be one of: " + VALID_COMPRESSIONS);
        }
        if (config.getFlushRows() <= 0) {
            errors.add("flushRows must be > 0");
        }
        if (config.getFlushIntervalSec() <= 0) {
            errors.add("flushIntervalSec must be > 0");
        }

        // Validate schemaJson
        errors.addAll(validateSchema(config.getSchemaJson()));
        return errors;
    }

    public List<String> validateSchema(String schemaJson) {
        List<String> errors = new ArrayList<>();
        if (schemaJson == null || schemaJson.trim().isEmpty()) {
            errors.add("schemaJson is required");
            return errors;
        }
        if (schemaJson.length() > MAX_SCHEMA_JSON_LEN) {
            errors.add("schemaJson exceeds max length " + MAX_SCHEMA_JSON_LEN);
            return errors;
        }
        try {
            SchemaDefinition schema = JSON.parseObject(schemaJson, SchemaDefinition.class);
            if (schema.getFields() == null || schema.getFields().isEmpty()) {
                errors.add("schemaJson must contain at least one field");
                return errors;
            }
            for (int i = 0; i < schema.getFields().size(); i++) {
                com.lakewriter.common.model.FieldDef f = schema.getFields().get(i);
                if (f.getName() == null || f.getName().trim().isEmpty()) {
                    errors.add("Field[" + i + "] name is required");
                }
                if (!VALID_TYPES.contains(f.getType())) {
                    errors.add("Field[" + i + "] type must be one of: " + VALID_TYPES);
                }
            }
        } catch (JSONException e) {
            errors.add("schemaJson is not valid JSON");
        }
        return errors;
    }

    /**
     * Validate a regex pattern for correctness and ReDoS safety.
     */
    private List<String> validateRegex(String pattern) {
        List<String> errors = new ArrayList<>();
        if (pattern.length() > MAX_REGEX_LEN) {
            errors.add("REGEX topicName exceeds max length " + MAX_REGEX_LEN);
            return errors;
        }
        try {
            Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            errors.add("Invalid regex: " + e.getDescription());
            return errors;
        }
        if (DANGEROUS_REGEX.matcher(pattern).matches()) {
            errors.add("Regex contains nested quantifiers which can cause catastrophic backtracking");
        }
        return errors;
    }

    public boolean isValidPathTemplate(String path) {
        if (path == null) return false;
        if (path.contains("..")) return false;
        return path.startsWith("/") || path.startsWith("oss://") || path.startsWith("s3://");
    }
}
