package com.lakewriter.admin.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    public List<String> validate(TopicSinkConfig config) {
        List<String> errors = new ArrayList<>();

        if (config.getTopicName() == null || config.getTopicName().trim().isEmpty()) {
            errors.add("topicName is required");
        }
        if (config.getSinkPath() == null || config.getSinkPath().trim().isEmpty()) {
            errors.add("sinkPath is required");
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
            errors.add("schemaJson is not valid JSON: " + e.getMessage());
        }
        return errors;
    }

    public boolean isValidPathTemplate(String path) {
        // Basic check: must start with / or oss://
        return path != null && (path.startsWith("/") || path.startsWith("oss://") || path.startsWith("s3://"));
    }
}
