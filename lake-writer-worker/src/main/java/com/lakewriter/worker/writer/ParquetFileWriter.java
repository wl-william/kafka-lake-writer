package com.lakewriter.worker.writer;

import com.lakewriter.common.model.FieldDef;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.storage.StorageAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet format writer.
 * RowGroup: 128MB, Page: 1MB, Snappy or GZIP, dictionary encoding enabled.
 * Compatible with parquet-hadoop 1.12.3 + CDH6 hadoop 3.0.0.
 */
@Slf4j
public class ParquetFileWriter implements FormatWriter {

    private ParquetWriter<Group> writer;
    private MessageType messageType;
    private SimpleGroupFactory factory;
    private long writtenRows = 0;

    @Override
    public void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException {
        this.messageType = buildMessageType(schema);
        this.factory = new SimpleGroupFactory(messageType);

        Configuration conf = new Configuration();
        CompressionCodecName codec = resolveCodec(config.getCompression());

        this.writer = ExampleParquetWriter.builder(new Path(filePath))
            .withType(messageType)
            .withConf(conf)
            .withRowGroupSize(128 * 1024 * 1024L)
            .withPageSize(1024 * 1024)
            .withCompressionCodec(codec)
            .withDictionaryPageSize(1024 * 1024)
            .withDictionaryEncoding(true)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
            .build();

        log.debug("Opened Parquet writer: path={}, codec={}", filePath, codec);
    }

    @Override
    public void writeRow(Object[] row) throws IOException {
        Group group = factory.newGroup();
        List<Type> fields = messageType.getFields();
        for (int i = 0; i < row.length && i < fields.size(); i++) {
            if (row[i] == null) continue;
            addFieldToGroup(group, fields.get(i), row[i]);
        }
        writer.write(group);
        writtenRows++;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public long getWrittenBytes() {
        return writer != null ? writer.getDataSize() : 0;
    }

    @Override
    public long getWrittenRows() {
        return writtenRows;
    }

    private void addFieldToGroup(Group group, Type type, Object val) {
        String name = type.getName();
        PrimitiveType.PrimitiveTypeName ptn = ((PrimitiveType) type).getPrimitiveTypeName();
        if (ptn == PrimitiveType.PrimitiveTypeName.INT64) {
            group.add(name, ((Number) val).longValue());
        } else if (ptn == PrimitiveType.PrimitiveTypeName.INT32) {
            group.add(name, ((Number) val).intValue());
        } else if (ptn == PrimitiveType.PrimitiveTypeName.FLOAT) {
            group.add(name, ((Number) val).floatValue());
        } else if (ptn == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            group.add(name, ((Number) val).doubleValue());
        } else if (ptn == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            group.add(name, (Boolean) val);
        } else {
            group.add(name, val.toString());
        }
    }

    public static MessageType buildMessageType(SchemaDefinition schema) {
        List<Type> types = new ArrayList<>();
        for (FieldDef f : schema.getFields()) {
            Type.Repetition rep = f.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
            // JDK 8: if-else instead of switch expression
            if ("LONG".equals(f.getType())) {
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.INT64, f.getName()));
            } else if ("INT".equals(f.getType())) {
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.INT32, f.getName()));
            } else if ("FLOAT".equals(f.getType())) {
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.FLOAT, f.getName()));
            } else if ("DOUBLE".equals(f.getType())) {
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.DOUBLE, f.getName()));
            } else if ("BOOLEAN".equals(f.getType())) {
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.BOOLEAN, f.getName()));
            } else {
                // STRING and unknown types -> BINARY UTF8
                types.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.BINARY, f.getName(), OriginalType.UTF8));
            }
        }
        return new MessageType("record", types);
    }

    private CompressionCodecName resolveCodec(String compression) {
        if ("GZIP".equalsIgnoreCase(compression)) return CompressionCodecName.GZIP;
        if ("NONE".equalsIgnoreCase(compression)) return CompressionCodecName.UNCOMPRESSED;
        return CompressionCodecName.SNAPPY;  // default
    }
}
