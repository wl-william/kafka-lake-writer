package com.lakewriter.admin.service;

import com.alibaba.fastjson2.JSON;
import com.lakewriter.common.model.ConfigChangeLog;
import com.lakewriter.common.model.TopicSinkConfig;
import com.lakewriter.common.repository.ConfigChangeLogRepository;
import com.lakewriter.common.repository.TopicSinkConfigRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Date;
import java.util.List;

/**
 * Business logic for Topic configuration management.
 * Every write operation creates a ConfigChangeLog entry for audit.
 */
@Slf4j
@Service
@Transactional
public class ConfigService {

    private final TopicSinkConfigRepository configRepo;
    private final ConfigChangeLogRepository changeLogRepo;

    public ConfigService(TopicSinkConfigRepository configRepo,
                         ConfigChangeLogRepository changeLogRepo) {
        this.configRepo = configRepo;
        this.changeLogRepo = changeLogRepo;
    }

    @Transactional(readOnly = true)
    public Page<TopicSinkConfig> findAll(Pageable pageable) {
        return configRepo.findAll(pageable);
    }

    @Transactional(readOnly = true)
    public Page<TopicSinkConfig> findByStatus(String status, Pageable pageable) {
        return configRepo.findByStatus(status, pageable);
    }

    @Transactional(readOnly = true)
    public TopicSinkConfig findById(Long id) {
        return configRepo.findById(id)
            .orElseThrow(() -> new RuntimeException("Config not found: " + id));
    }

    public TopicSinkConfig create(TopicSinkConfig config) {
        config.setVersion(1);
        config.setStatus("ACTIVE");
        config.setCreatedAt(new Date());
        config.setUpdatedAt(new Date());
        TopicSinkConfig saved = configRepo.save(config);
        writeLog(saved, null, JSON.toJSONString(saved), "CREATED");
        log.info("Config created: topic={}", saved.getTopicName());
        return saved;
    }

    public TopicSinkConfig update(Long id, TopicSinkConfig update) {
        TopicSinkConfig existing = findById(id);
        String before = JSON.toJSONString(existing);

        existing.setSchemaJson(update.getSchemaJson());
        existing.setSinkPath(update.getSinkPath());
        existing.setSinkFormat(update.getSinkFormat());
        existing.setCompression(update.getCompression());
        existing.setFlushRows(update.getFlushRows());
        existing.setFlushBytes(update.getFlushBytes());
        existing.setFlushIntervalSec(update.getFlushIntervalSec());
        existing.setPartitionBy(update.getPartitionBy());
        existing.setDescription(update.getDescription());
        existing.setVersion(existing.getVersion() + 1);  // version++ triggers Worker detection
        existing.setUpdatedAt(new Date());

        TopicSinkConfig saved = configRepo.save(existing);
        writeLog(saved, before, JSON.toJSONString(saved), "UPDATED");
        log.info("Config updated: topic={}, version={}", saved.getTopicName(), saved.getVersion());
        return saved;
    }

    public void delete(Long id) {
        TopicSinkConfig existing = findById(id);
        String before = JSON.toJSONString(existing);
        configRepo.deleteById(id);
        writeLog(existing, before, null, "DELETED");
        log.info("Config deleted: topic={}", existing.getTopicName());
    }

    public TopicSinkConfig pause(Long id) {
        TopicSinkConfig existing = findById(id);
        existing.setStatus("PAUSED");
        existing.setVersion(existing.getVersion() + 1);
        existing.setUpdatedAt(new Date());
        TopicSinkConfig saved = configRepo.save(existing);
        writeLog(saved, null, null, "PAUSED");
        return saved;
    }

    public TopicSinkConfig resume(Long id) {
        TopicSinkConfig existing = findById(id);
        existing.setStatus("ACTIVE");
        existing.setVersion(existing.getVersion() + 1);
        existing.setUpdatedAt(new Date());
        TopicSinkConfig saved = configRepo.save(existing);
        writeLog(saved, null, null, "RESUMED");
        return saved;
    }

    @Transactional(readOnly = true)
    public List<ConfigChangeLog> getChangelog(Long configId) {
        return changeLogRepo.findByConfigIdOrderByCreatedAtDesc(configId);
    }

    private void writeLog(TopicSinkConfig config, String before, String after, String type) {
        ConfigChangeLog log = new ConfigChangeLog();
        log.setConfigId(config.getId());
        log.setTopicName(config.getTopicName());
        log.setChangeType(type);
        log.setOperator(currentUsername());
        log.setBeforeJson(before);
        log.setAfterJson(after);
        log.setCreatedAt(new Date());
        changeLogRepo.save(log);
    }

    private String currentUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return (auth != null && auth.getName() != null) ? auth.getName() : "system";
    }
}
