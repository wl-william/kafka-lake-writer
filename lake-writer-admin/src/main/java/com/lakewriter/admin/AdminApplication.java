package com.lakewriter.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Admin service entry point — Web server, no Kafka, no HDFS.
 * Reads/writes MySQL: topic_sink_config, config_change_log.
 * Reads MySQL: worker_heartbeat (written by Workers).
 */
@SpringBootApplication(scanBasePackages = {"com.lakewriter.admin", "com.lakewriter.common"})
@EntityScan(basePackages = "com.lakewriter.common.model")
@EnableJpaRepositories(basePackages = "com.lakewriter.common.repository")
@EnableScheduling
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}
