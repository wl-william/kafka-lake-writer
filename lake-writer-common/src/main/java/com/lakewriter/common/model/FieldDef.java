package com.lakewriter.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Defines a single field in a topic's schema.
 * Supported types: STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldDef {
    private String name;
    private String type;      // STRING | INT | LONG | FLOAT | DOUBLE | BOOLEAN
    private boolean nullable = true;
}
