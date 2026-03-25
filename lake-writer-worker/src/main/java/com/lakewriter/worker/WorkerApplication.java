package com.lakewriter.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Worker entry point — pure Kafka consumer, no Web server.
 * web-application-type: none in application.yml ensures no Tomcat starts.
 * Only actuator endpoints are exposed (/health, /prometheus).
 */
@Slf4j
@SpringBootApplication(scanBasePackages = {"com.lakewriter.worker", "com.lakewriter.common"})
@EntityScan(basePackages = "com.lakewriter.common.model")
@EnableJpaRepositories(basePackages = "com.lakewriter.common.repository")
@EnableScheduling
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
        log.info("=== Lake Writer Worker started ===");
    }
}
