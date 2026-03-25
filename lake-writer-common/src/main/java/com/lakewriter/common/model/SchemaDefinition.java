package com.lakewriter.common.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema definition wrapper — serialized as {"fields":[...]} in topic_sink_config.schema_json.
 */
@Data
public class SchemaDefinition {
    private List<FieldDef> fields = new ArrayList<>();
}
