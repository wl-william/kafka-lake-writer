package com.lakewriter.common.repository;

import com.lakewriter.common.model.ConfigChangeLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ConfigChangeLogRepository extends JpaRepository<ConfigChangeLog, Long> {

    List<ConfigChangeLog> findByConfigIdOrderByCreatedAtDesc(Long configId);

    List<ConfigChangeLog> findByTopicNameOrderByCreatedAtDesc(String topicName);
}
