package com.lakewriter.worker.writer;

import com.lakewriter.common.model.FieldDef;
import com.lakewriter.common.model.SchemaDefinition;
import com.lakewriter.common.model.TopicSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * CSV format writer using Apache Commons CSV.
 * Supports GZIP compression transparently.
 */
@Slf4j
public class CsvFileWriter implements FormatWriter {

    private final com.lakewriter.worker.storage.StorageAdapter storage;
    private CSVPrinter printer;
    private OutputStream outputStream;
    private long writtenRows = 0;
    private long writtenBytes = 0;

    public CsvFileWriter(com.lakewriter.worker.storage.StorageAdapter storage) {
        this.storage = storage;
    }

    @Override
    public void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException {
        outputStream = storage.create(filePath);

        OutputStream out;
        if ("GZIP".equalsIgnoreCase(config.getCompression())) {
            out = new GZIPOutputStream(outputStream);
        } else {
            out = outputStream;
        }

        // Build header from schema fields
        List<FieldDef> fields = schema.getFields();
        String[] headers = fields.stream().map(FieldDef::getName).toArray(String[]::new);

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        this.printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headers));
        log.debug("Opened CSV writer: path={}, compression={}", filePath, config.getCompression());
    }

    @Override
    public void writeRow(Object[] row) throws IOException {
        printer.printRecord(row);
        writtenRows++;
        for (Object o : row) {
            if (o != null) writtenBytes += o.toString().length();
        }
    }

    @Override
    public void close() throws IOException {
        if (printer != null) {
            printer.flush();
            printer.close();
        }
    }

    @Override
    public long getWrittenBytes() {
        return writtenBytes;
    }

    @Override
    public long getWrittenRows() {
        return writtenRows;
    }
}
