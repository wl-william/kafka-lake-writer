package com.lakewriter.common.repository;

import com.lakewriter.common.model.TopicSinkConfig;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TopicSinkConfigRepository extends JpaRepository<TopicSinkConfig, Long> {

    Optional<TopicSinkConfig> findByTopicName(String topicName);

    List<TopicSinkConfig> findByStatus(String status);

    Page<TopicSinkConfig> findByStatus(String status, Pageable pageable);

    /** Used by Worker ConfigPoller — only pulls configs Workers need to consume */
    @Query("SELECT t FROM TopicSinkConfig t WHERE t.status = 'ACTIVE'")
    List<TopicSinkConfig> findAllActive();
}
