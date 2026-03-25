package com.lakewriter.worker.schema;

import com.lakewriter.common.model.FieldDef;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.worker.writer.ParquetFileWriter;
import org.apache.parquet.schema.MessageType;

/**
 * Converts SchemaDefinition to format-specific schemas.
 * Centralizes schema conversion so writers don't need to duplicate this logic.
 */
public class DynamicSchemaConverter {

    /** Convert to Parquet MessageType (delegates to ParquetFileWriter utility) */
    public MessageType toParquetSchema(SchemaDefinition schema) {
        return ParquetFileWriter.buildMessageType(schema);
    }

    /** Parse schemaJson string into SchemaDefinition */
    public SchemaDefinition parse(String schemaJson) {
        return com.alibaba.fastjson2.JSON.parseObject(schemaJson, SchemaDefinition.class);
    }

    /** Convenience: schema JSON string -> field array */
    public FieldDef[] toFieldArray(String schemaJson) {
        SchemaDefinition sd = parse(schemaJson);
        return sd.getFields().toArray(new FieldDef[0]);
    }
}
