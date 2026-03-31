package com.lakewriter.worker.writer;

import com.lakewriter.common.model.FieldDef;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.worker.storage.HadoopStorageAdapter;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetFileWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void openUsesStorageConfigurationAndCreatesParentDirectories() throws IOException {
        Configuration conf = new Configuration();
        HadoopStorageAdapter storage = Mockito.spy(new HadoopStorageAdapter(conf, "file:///"));
        ParquetFileWriter writer = new ParquetFileWriter(storage);

        String filePath = tempDir.resolve("nested/test.parquet").toString();
        writer.open(filePath, schema(), parquetConfig());
        writer.writeRow(new Object[]{1L, "alice"});
        writer.writeRow(new Object[]{2L, "bob"});
        writer.close();

        Mockito.verify(storage).getConf();
        Mockito.verify(storage).mkdirs(Mockito.argThat(argument -> argument != null && argument.endsWith("nested")));
        assertTrue(new File(filePath).exists());
        assertEquals(2, writer.getWrittenRows());
    }

    private SchemaDefinition schema() {
        SchemaDefinition schema = new SchemaDefinition();
        schema.setFields(Arrays.asList(
            new FieldDef("id", "LONG", false),
            new FieldDef("name", "STRING", true)
        ));
        return schema;
    }

    private TopicSinkConfig parquetConfig() {
        TopicSinkConfig config = new TopicSinkConfig();
        config.setCompression("GZIP");
        config.setSinkFormat("PARQUET");
        return config;
    }
}
