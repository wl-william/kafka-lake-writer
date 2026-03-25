package com.lakewriter.worker.checkpoint;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads and writes checkpoint files on local disk.
 * Layout: {baseDir}/{topic}/{partition}.ckpt
 */
@Slf4j
public class CheckpointManager {

    private final String checkpointBaseDir;

    public CheckpointManager(String baseDir) {
        this.checkpointBaseDir = baseDir;
        new File(baseDir).mkdirs();
    }

    public void save(Checkpoint ckpt) throws IOException {
        File dir = new File(checkpointBaseDir, ckpt.getTopic());
        dir.mkdirs();
        File file = new File(dir, ckpt.getPartition() + ".ckpt");
        try (FileWriter fw = new FileWriter(file)) {
            fw.write(ckpt.toJson());
        }
        log.debug("Saved checkpoint: topic={}, partition={}, offsets={}-{}",
            ckpt.getTopic(), ckpt.getPartition(), ckpt.getStartOffset(), ckpt.getEndOffset());
    }

    public Optional<Checkpoint> load(TopicPartition tp) {
        File file = ckptFile(tp);
        if (!file.exists()) return Optional.empty();
        try {
            String json = new String(Files.readAllBytes(file.toPath()));
            return Optional.of(Checkpoint.fromJson(json));
        } catch (IOException e) {
            log.warn("Failed to read checkpoint file: {}", file.getAbsolutePath(), e);
            return Optional.empty();
        }
    }

    public void delete(TopicPartition tp) {
        File file = ckptFile(tp);
        if (file.exists() && file.delete()) {
            log.debug("Deleted checkpoint: {}", tp);
        }
    }

    /** Load all checkpoint files at startup for crash recovery */
    public List<Checkpoint> loadAll() {
        List<Checkpoint> result = new ArrayList<>();
        File base = new File(checkpointBaseDir);
        if (!base.exists()) return result;

        File[] topicDirs = base.listFiles(File::isDirectory);
        if (topicDirs == null) return result;

        for (File topicDir : topicDirs) {
            File[] ckptFiles = topicDir.listFiles(f -> f.getName().endsWith(".ckpt"));
            if (ckptFiles == null) continue;
            for (File f : ckptFiles) {
                try {
                    String json = new String(Files.readAllBytes(f.toPath()));
                    result.add(Checkpoint.fromJson(json));
                } catch (IOException e) {
                    log.warn("Skipping unreadable checkpoint: {}", f.getAbsolutePath());
                }
            }
        }
        return result;
    }

    private File ckptFile(TopicPartition tp) {
        return new File(new File(checkpointBaseDir, tp.topic()), tp.partition() + ".ckpt");
    }
}
