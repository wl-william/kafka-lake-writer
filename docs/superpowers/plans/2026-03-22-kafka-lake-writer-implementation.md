# Kafka-Lake-Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight, high-performance Kafka-to-HDFS/OSS sync service (60k records/s on 4C8G) with dynamic config, At-Least-Once semantics, multi-node deployment, and a separate web management UI.

**Architecture:** Maven multi-module project: `lake-writer-common` (shared entities/repos), `lake-writer-worker` (pure Kafka consumer, no web), `lake-writer-admin` (Spring Boot Web + Vue.js UI). Admin and Worker communicate only through MySQL — zero direct calls.

**Tech Stack:** JDK 1.8, Spring Boot 2.7.18, kafka-clients 2.8.2, parquet-hadoop 1.12.3, hadoop-client 3.0.0-cdh6.3.1, fastjson2 2.0.49, Vue.js 2.x + Element UI, MySQL 5.7+

---

## File Map

### Root / Infrastructure
| Action | File | Responsibility |
|--------|------|----------------|
| Create | `pom.xml` | Parent POM, version management, CDH6 repo |
| Create | `sql/init.sql` | DDL for all 3 tables |
| Create | `docker/Dockerfile.worker` | Worker image |
| Create | `docker/Dockerfile.admin` | Admin image |
| Create | `docker/docker-compose.yml` | Local dev env |

### lake-writer-common
| Action | File | Responsibility |
|--------|------|----------------|
| Create | `lake-writer-common/pom.xml` | Module POM |
| Create | `...common/model/TopicSinkConfig.java` | JPA entity, config table |
| Create | `...common/model/FieldDef.java` | Schema field definition |
| Create | `...common/model/SchemaDefinition.java` | Fields array wrapper |
| Create | `...common/model/ConfigChangeLog.java` | Audit log entity |
| Create | `...common/model/WorkerHeartbeat.java` | Heartbeat entity |
| Create | `...common/repository/TopicSinkConfigRepository.java` | JPA repo |
| Create | `...common/repository/ConfigChangeLogRepository.java` | JPA repo |
| Create | `...common/repository/WorkerHeartbeatRepository.java` | JPA repo + upsert |

### lake-writer-worker
| Action | File | Responsibility |
|--------|------|----------------|
| Create | `lake-writer-worker/pom.xml` | Module POM, depends on common |
| Create | `...worker/WorkerApplication.java` | Spring Boot entry, web-application-type: none |
| Create | `...worker/node/NodeIdentity.java` | POD_NAME > HOSTNAME > random |
| Create | `...worker/storage/StorageAdapter.java` | Interface: create/rename/exists/delete/list |
| Create | `...worker/storage/HadoopStorageAdapter.java` | HDFS impl via Hadoop FileSystem API |
| Create | `...worker/writer/FormatWriter.java` | Interface: open/writeRow/close/getWrittenBytes |
| Create | `...worker/writer/ParquetFileWriter.java` | Parquet impl, 128MB RowGroup, Snappy |
| Create | `...worker/writer/CsvFileWriter.java` | CSV impl via commons-csv |
| Create | `...worker/writer/FileWriterFactory.java` | Creates writer by format type |
| Create | `...worker/writer/FlushExecutor.java` | 5-phase commit + retry (2s/4s/8s) + degraded mode |
| Create | `...worker/schema/DynamicSchemaConverter.java` | JSON schema → Parquet MessageType |
| Create | `...worker/schema/JsonRecordParser.java` | fastjson2 batch parse → Object[] rows |
| Create | `...worker/schema/PathResolver.java` | Template engine: {date}/{topic}/{dt}/{hour} |
| Create | `...worker/buffer/WriteBuffer.java` | Single-partition buffer, tracks offsets + bytes |
| Create | `...worker/buffer/DoubleWriteBuffer.java` | Swap pattern: active/flushing buffers |
| Create | `...worker/buffer/WriteBufferManager.java` | All partitions' buffers, lifecycle management |
| Create | `...worker/buffer/BackpressureController.java` | Pause consumer when total bytes > 3GB |
| Create | `...worker/checkpoint/Checkpoint.java` | Checkpoint POJO + JSON serialization |
| Create | `...worker/checkpoint/CheckpointManager.java` | Read/write/delete local checkpoint files |
| Create | `...worker/checkpoint/CrashRecoveryManager.java` | Startup recovery: 7 crash scenarios |
| Create | `...worker/checkpoint/IdempotentWriteChecker.java` | Index filenames by offset range |
| Create | `...worker/consumer/KafkaConsumerPool.java` | N consumer threads, group management |
| Create | `...worker/consumer/ConsumerWorker.java` | Single consumer thread poll loop |
| Create | `...worker/consumer/RecordDispatcher.java` | Route records to correct buffer via TopicMatcher |
| Create | `...worker/consumer/SafeRebalanceListener.java` | onPartitionsRevoked: emergency flush |
| Create | `...worker/config/ConfigPoller.java` | 30s scheduled poll, triggers diff handling |
| Create | `...worker/config/ConfigDiff.java` | Diff algorithm: added/removed/schemaChanged/otherChanged |
| Create | `...worker/config/TopicMatcher.java` | EXACT + REGEX matching |
| Create | `...worker/heartbeat/HeartbeatReporter.java` | @Scheduled 15s upsert to worker_heartbeat |
| Create | `...worker/config/VersionedConfigSync.java` | Delays subscription changes 60s to align all nodes, reduce rebalance storms |
| Create | `...worker/metrics/WriterMetrics.java` | Micrometer counters/timers for Prometheus |
| Create | `...worker/lifecycle/GracefulShutdownHook.java` | @PreDestroy: stop→flush→commit→close |
| Create | `...worker/resources/application.yml` | Kafka/HDFS/MySQL config, web-type: none |

### lake-writer-admin
| Action | File | Responsibility |
|--------|------|----------------|
| Create | `lake-writer-admin/pom.xml` | Module POM, depends on common |
| Create | `...admin/AdminApplication.java` | Spring Boot entry, full web |
| Create | `...admin/controller/ConfigController.java` | CRUD: GET/POST/PUT/DELETE /api/v1/configs |
| Create | `...admin/controller/StatusController.java` | GET /api/v1/status, /nodes |
| Create | `...admin/controller/ChangeLogController.java` | GET /api/v1/configs/{id}/changelog |
| Create | `...admin/service/ConfigService.java` | Biz logic: save config + write changelog |
| Create | `...admin/service/ConfigValidator.java` | Validate schema JSON + path template |
| Create | `...admin/service/StatusAggregator.java` | Read heartbeat table, mark OFFLINE if >60s |
| Create | `...admin/config/WebMvcConfig.java` | SPA fallback routing |
| Create | `...admin/resources/application.yml` | MySQL only, port 8080 |
| Create | `frontend/package.json` | Vue CLI, element-ui, axios, echarts |
| Create | `frontend/src/main.js` | Vue + Router + Element UI setup |
| Create | `frontend/src/router/index.js` | 5 routes |
| Create | `frontend/src/api/config.js` | Axios calls for config CRUD |
| Create | `frontend/src/api/status.js` | Axios calls for status/nodes |
| Create | `frontend/src/views/Dashboard.vue` | 4 stat cards + lag table |
| Create | `frontend/src/views/ConfigList.vue` | Table + pause/resume/delete actions |
| Create | `frontend/src/views/ConfigEdit.vue` | Form with SchemaEditor + PathPreview |
| Create | `frontend/src/views/MonitorDetail.vue` | Partition progress + buffer status |
| Create | `frontend/src/views/ChangeLog.vue` | Audit log table |
| Create | `frontend/src/views/NodeList.vue` | Worker node online/offline status |
| Create | `frontend/src/components/SchemaEditor.vue` | Add/remove/reorder fields table |
| Create | `frontend/src/components/PathPreview.vue` | Real-time path template preview |
| Create | `frontend/src/components/ConsumerLagChart.vue` | ECharts lag trend line |

---

## Task 1: Maven Multi-Module Scaffolding + SQL

**Files:**
- Create: `pom.xml`
- Create: `lake-writer-common/pom.xml`
- Create: `lake-writer-worker/pom.xml`
- Create: `lake-writer-admin/pom.xml`
- Create: `sql/init.sql`

- [ ] **Step 1: Create parent POM**

```xml
<!-- kafka-lake-writer/pom.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.lakewriter</groupId>
    <artifactId>kafka-lake-writer</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <module>lake-writer-common</module>
        <module>lake-writer-worker</module>
        <module>lake-writer-admin</module>
    </modules>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.7.18</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>1.8</java.version>
        <kafka.version>2.8.2</kafka.version>
        <parquet.version>1.12.3</parquet.version>
        <hadoop.version>3.0.0-cdh6.3.1</hadoop.version>
        <fastjson2.version>2.0.49</fastjson2.version>
    </properties>

    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.lakewriter</groupId>
                <artifactId>lake-writer-common</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-clients</artifactId>
                <version>${kafka.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.parquet</groupId>
                <artifactId>parquet-hadoop</artifactId>
                <version>${parquet.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.parquet</groupId>
                <artifactId>parquet-avro</artifactId>
                <version>${parquet.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-client</artifactId>
                <version>${hadoop.version}</version>
                <exclusions>
                    <exclusion>
                        <groupId>org.slf4j</groupId>
                        <artifactId>slf4j-log4j12</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
            <dependency>
                <groupId>com.alibaba.fastjson2</groupId>
                <artifactId>fastjson2</artifactId>
                <version>${fastjson2.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.commons</groupId>
                <artifactId>commons-csv</artifactId>
                <version>1.10.0</version>
            </dependency>
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>8.0.33</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <configuration>
                        <source>1.8</source>
                        <target>1.8</target>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] **Step 2: Create common module POM**

```xml
<!-- lake-writer-common/pom.xml -->
<project xmlns="http://maven.apache.org/POM/4.0.0" ...>
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.lakewriter</groupId>
        <artifactId>kafka-lake-writer</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>lake-writer-common</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba.fastjson2</groupId>
            <artifactId>fastjson2</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: Create worker module POM**

```xml
<!-- lake-writer-worker/pom.xml -->
<dependencies>
    <dependency>
        <groupId>com.lakewriter</groupId>
        <artifactId>lake-writer-common</artifactId>
    </dependency>
    <dependency><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-hadoop</artifactId></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-avro</artifactId></dependency>
    <dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-client</artifactId></dependency>
    <dependency><groupId>com.alibaba.fastjson2</groupId><artifactId>fastjson2</artifactId></dependency>
    <dependency><groupId>org.apache.commons</groupId><artifactId>commons-csv</artifactId></dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

- [ ] **Step 4: Create admin module POM**

```xml
<!-- lake-writer-admin/pom.xml -->
<dependencies>
    <dependency>
        <groupId>com.lakewriter</groupId>
        <artifactId>lake-writer-common</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

- [ ] **Step 5: Create sql/init.sql with full DDL**

```sql
-- sql/init.sql
CREATE TABLE IF NOT EXISTS `topic_sink_config` (
    `id`                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    `topic_name`         VARCHAR(255) NOT NULL,
    `match_type`         VARCHAR(16)  NOT NULL DEFAULT 'EXACT',
    `schema_json`        TEXT         NOT NULL,
    `sink_format`        VARCHAR(16)  NOT NULL DEFAULT 'PARQUET',
    `sink_path`          VARCHAR(512) NOT NULL,
    `partition_by`       VARCHAR(255)          DEFAULT NULL,
    `compression`        VARCHAR(32)  NOT NULL DEFAULT 'SNAPPY',
    `flush_rows`         INT          NOT NULL DEFAULT 500000,
    `flush_bytes`        BIGINT       NOT NULL DEFAULT 268435456,
    `flush_interval_sec` INT          NOT NULL DEFAULT 600,
    `status`             VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    `version`            INT          NOT NULL DEFAULT 1,
    `description`        VARCHAR(512)          DEFAULT NULL,
    `created_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_topic_name` (`topic_name`),
    KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `config_change_log` (
    `id`          BIGINT AUTO_INCREMENT PRIMARY KEY,
    `config_id`   BIGINT       NOT NULL,
    `topic_name`  VARCHAR(255) NOT NULL,
    `change_type` VARCHAR(32)  NOT NULL,
    `operator`    VARCHAR(64)  DEFAULT 'admin',
    `before_json` TEXT,
    `after_json`  TEXT,
    `created_at`  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY `idx_config_id` (`config_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `worker_heartbeat` (
    `node_id`             VARCHAR(128) NOT NULL PRIMARY KEY,
    `status`              VARCHAR(16)  NOT NULL DEFAULT 'ONLINE',
    `assigned_partitions` TEXT,
    `consumer_status`     TEXT,
    `records_per_sec`     DOUBLE       NOT NULL DEFAULT 0,
    `buffer_usage_bytes`  BIGINT       NOT NULL DEFAULT 0,
    `uptime_sec`          BIGINT       NOT NULL DEFAULT 0,
    `last_heartbeat`      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 6: Verify Maven build structure compiles**

```bash
cd kafka-lake-writer && mvn compile -pl lake-writer-common
```
Expected: `BUILD SUCCESS`

- [ ] **Step 7: Commit**

```bash
git add pom.xml lake-writer-common/pom.xml lake-writer-worker/pom.xml lake-writer-admin/pom.xml sql/
git commit -m "feat: bootstrap maven multi-module project structure and DB schema"
```

---

## Task 2: Common Module — Entities & Repositories

**Files:**
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/model/TopicSinkConfig.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/model/FieldDef.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/model/SchemaDefinition.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/model/ConfigChangeLog.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/model/WorkerHeartbeat.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/repository/TopicSinkConfigRepository.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/repository/ConfigChangeLogRepository.java`
- Create: `lake-writer-common/src/main/java/com/lakewriter/common/repository/WorkerHeartbeatRepository.java`
- Test: `lake-writer-common/src/test/java/com/lakewriter/common/model/TopicSinkConfigTest.java`

- [ ] **Step 1: Write failing test for TopicSinkConfig**

```java
// TopicSinkConfigTest.java
public class TopicSinkConfigTest {
    @Test
    public void testSchemaJsonParsing() {
        TopicSinkConfig config = new TopicSinkConfig();
        config.setSchemaJson("{\"fields\":[{\"name\":\"id\",\"type\":\"LONG\",\"nullable\":false}]}");
        SchemaDefinition schema = config.parseSchema();
        assertEquals(1, schema.getFields().size());
        assertEquals("id", schema.getFields().get(0).getName());
        assertEquals("LONG", schema.getFields().get(0).getType());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-common -Dtest=TopicSinkConfigTest
```
Expected: FAIL — class not found

- [ ] **Step 3: Implement entities**

```java
// FieldDef.java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldDef {
    private String name;
    private String type;     // STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN
    private boolean nullable = true;
}

// SchemaDefinition.java
@Data
public class SchemaDefinition {
    private List<FieldDef> fields = new ArrayList<>();
}

// TopicSinkConfig.java
@Data
@Entity
@Table(name = "topic_sink_config")
public class TopicSinkConfig {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String topicName;
    private String matchType = "EXACT";  // EXACT | REGEX
    @Column(columnDefinition = "TEXT")
    private String schemaJson;
    private String sinkFormat = "PARQUET";   // PARQUET | CSV
    private String sinkPath;
    private String partitionBy;
    private String compression = "SNAPPY";
    private int flushRows = 500000;
    private long flushBytes = 268435456L;
    private int flushIntervalSec = 600;
    private String status = "ACTIVE";
    private int version = 1;
    private String description;
    private Date createdAt;
    private Date updatedAt;

    @Transient
    public SchemaDefinition parseSchema() {
        return JSON.parseObject(this.schemaJson, SchemaDefinition.class);
    }
}

// ConfigChangeLog.java
@Data
@Entity
@Table(name = "config_change_log")
public class ConfigChangeLog {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long configId;
    private String topicName;
    private String changeType;   // CREATED, UPDATED, DELETED, PAUSED, RESUMED
    private String operator;
    @Column(columnDefinition = "TEXT")
    private String beforeJson;
    @Column(columnDefinition = "TEXT")
    private String afterJson;
    private Date createdAt;
}

// WorkerHeartbeat.java
@Data
@Entity
@Table(name = "worker_heartbeat")
public class WorkerHeartbeat {
    @Id
    private String nodeId;
    private String status;       // ONLINE | OFFLINE
    @Column(columnDefinition = "TEXT")
    private String assignedPartitions;  // JSON array
    @Column(columnDefinition = "TEXT")
    private String consumerStatus;      // JSON map
    private double recordsPerSec;
    private long bufferUsageBytes;
    private long uptimeSec;
    private Date lastHeartbeat;
}
```

- [ ] **Step 4: Implement repositories**

```java
// TopicSinkConfigRepository.java
public interface TopicSinkConfigRepository extends JpaRepository<TopicSinkConfig, Long> {
    List<TopicSinkConfig> findByStatus(String status);
    Optional<TopicSinkConfig> findByTopicName(String topicName);
    // findAllActive used by worker config poller
    @Query("SELECT t FROM TopicSinkConfig t WHERE t.status = 'ACTIVE'")
    List<TopicSinkConfig> findAllActive();
}

// ConfigChangeLogRepository.java
public interface ConfigChangeLogRepository extends JpaRepository<ConfigChangeLog, Long> {
    List<ConfigChangeLog> findByConfigIdOrderByCreatedAtDesc(Long configId);
}

// WorkerHeartbeatRepository.java
public interface WorkerHeartbeatRepository extends JpaRepository<WorkerHeartbeat, String> {
    // upsert via save() — Spring Data JPA handles INSERT or UPDATE by PK
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-common -Dtest=TopicSinkConfigTest
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add lake-writer-common/src/
git commit -m "feat(common): add JPA entities and repositories"
```

---

## Task 3: Worker — Storage Adapter

**Files:**
- Create: `...worker/storage/StorageAdapter.java`
- Create: `...worker/storage/HadoopStorageAdapter.java`
- Test: `...worker/storage/HadoopStorageAdapterTest.java`

- [ ] **Step 1: Write failing test**

```java
// HadoopStorageAdapterTest.java — use in-process HDFS (MiniDFSCluster) or mock FileSystem
public class HadoopStorageAdapterTest {
    private HadoopStorageAdapter adapter;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");   // local FS for unit test
        adapter = new HadoopStorageAdapter(conf, "file:///");
    }

    @Test
    public void testWriteAndExists() throws Exception {
        String path = System.getProperty("java.io.tmpdir") + "/test-adapter-" + System.currentTimeMillis() + ".txt";
        try (OutputStream os = adapter.create(path)) {
            os.write("hello".getBytes());
        }
        assertTrue(adapter.exists(path));
        adapter.delete(path);
        assertFalse(adapter.exists(path));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=HadoopStorageAdapterTest
```
Expected: FAIL

- [ ] **Step 3: Implement StorageAdapter interface and HadoopStorageAdapter**

```java
// StorageAdapter.java
public interface StorageAdapter extends AutoCloseable {
    OutputStream create(String path) throws IOException;
    boolean rename(String srcPath, String dstPath) throws IOException;
    boolean exists(String path) throws IOException;
    boolean delete(String path) throws IOException;
    List<String> listFiles(String dirPath) throws IOException;
    boolean mkdirs(String path) throws IOException;
}

// HadoopStorageAdapter.java
public class HadoopStorageAdapter implements StorageAdapter {
    private final FileSystem fs;

    public HadoopStorageAdapter(Configuration conf, String defaultFs) throws IOException {
        conf.set("fs.defaultFS", defaultFs);
        this.fs = FileSystem.get(conf);
    }

    @Override
    public OutputStream create(String path) throws IOException {
        Path p = new Path(path);
        fs.mkdirs(p.getParent());
        return fs.create(p, true);
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        Path dstPath = new Path(dst);
        fs.mkdirs(dstPath.getParent());
        return fs.rename(new Path(src), dstPath);
    }

    @Override
    public boolean exists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    @Override
    public boolean delete(String path) throws IOException {
        return fs.delete(new Path(path), false);
    }

    @Override
    public List<String> listFiles(String dirPath) throws IOException {
        if (!fs.exists(new Path(dirPath))) return Collections.emptyList();
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        List<String> result = new ArrayList<>();
        for (FileStatus s : statuses) {
            result.add(s.getPath().toString());
        }
        return result;
    }

    @Override
    public boolean mkdirs(String path) throws IOException {
        return fs.mkdirs(new Path(path));
    }

    @Override
    public void close() throws IOException {
        fs.close();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest=HadoopStorageAdapterTest
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add StorageAdapter interface and HadoopStorageAdapter"
```

---

## Task 4: Worker — Format Writers (Parquet + CSV)

**Files:**
- Create: `...writer/FormatWriter.java`
- Create: `...writer/ParquetFileWriter.java`
- Create: `...writer/CsvFileWriter.java`
- Create: `...writer/FileWriterFactory.java`
- Test: `...writer/ParquetFileWriterTest.java`
- Test: `...writer/CsvFileWriterTest.java`

- [ ] **Step 1: Write failing tests**

```java
// ParquetFileWriterTest.java
public class ParquetFileWriterTest {
    @Test
    public void testWriteAndRead() throws Exception {
        String tmpPath = System.getProperty("java.io.tmpdir") + "/test-" + System.nanoTime() + ".parquet";
        SchemaDefinition schema = new SchemaDefinition();
        schema.setFields(Arrays.asList(
            new FieldDef("id", "LONG", false),
            new FieldDef("name", "STRING", true)
        ));
        TopicSinkConfig config = new TopicSinkConfig();
        config.setCompression("SNAPPY");
        config.setSinkFormat("PARQUET");

        HadoopStorageAdapter adapter = new HadoopStorageAdapter(new Configuration(), "file:///");
        ParquetFileWriter writer = new ParquetFileWriter(adapter);
        writer.open(tmpPath, schema, config);
        writer.writeRow(new Object[]{1L, "alice"});
        writer.writeRow(new Object[]{2L, "bob"});
        writer.close();

        assertTrue(new File(tmpPath).exists());
        assertEquals(2, writer.getWrittenRows());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=ParquetFileWriterTest
```
Expected: FAIL

- [ ] **Step 3: Implement FormatWriter interface**

```java
public interface FormatWriter extends AutoCloseable {
    void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException;
    void writeRow(Object[] row) throws IOException;
    default void writeRows(List<Object[]> rows) throws IOException {
        for (Object[] row : rows) writeRow(row);
    }
    @Override
    void close() throws IOException;
    long getWrittenBytes();
    long getWrittenRows();
}
```

- [ ] **Step 4: Implement ParquetFileWriter**

```java
public class ParquetFileWriter implements FormatWriter {
    private final StorageAdapter storage;
    private ParquetWriter<Group> writer;
    private MessageType messageType;
    private GroupFactory factory;
    private long writtenRows = 0;

    public ParquetFileWriter(StorageAdapter storage) {
        this.storage = storage;
    }

    @Override
    public void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException {
        this.messageType = buildMessageType(schema);
        this.factory = new SimpleGroupFactory(messageType);
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        CompressionCodecName codec = "GZIP".equals(config.getCompression())
            ? CompressionCodecName.GZIP : CompressionCodecName.SNAPPY;
        this.writer = ExampleParquetWriter.builder(path)
            .withType(messageType)
            .withConf(conf)
            .withRowGroupSize(128 * 1024 * 1024)
            .withPageSize(1024 * 1024)
            .withCompressionCodec(codec)
            .withDictionaryEncoding(true)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build();
    }

    @Override
    public void writeRow(Object[] row) throws IOException {
        Group group = factory.newGroup();
        Type[] fields = messageType.getFields().toArray(new Type[0]);
        for (int i = 0; i < row.length; i++) {
            if (row[i] == null) continue;
            String name = fields[i].getName();
            addToGroup(group, name, fields[i], row[i]);
        }
        writer.write(group);
        writtenRows++;
    }

    private void addToGroup(Group group, String name, Type type, Object val) {
        PrimitiveType.PrimitiveTypeName ptn = ((PrimitiveType) type).getPrimitiveTypeName();
        if (ptn == PrimitiveType.PrimitiveTypeName.INT64)  group.add(name, ((Number) val).longValue());
        else if (ptn == PrimitiveType.PrimitiveTypeName.INT32) group.add(name, ((Number) val).intValue());
        else if (ptn == PrimitiveType.PrimitiveTypeName.FLOAT)  group.add(name, ((Number) val).floatValue());
        else if (ptn == PrimitiveType.PrimitiveTypeName.DOUBLE) group.add(name, ((Number) val).doubleValue());
        else if (ptn == PrimitiveType.PrimitiveTypeName.BOOLEAN) group.add(name, (Boolean) val);
        else group.add(name, val.toString());
    }

    private MessageType buildMessageType(SchemaDefinition schema) {
        List<Type> fields = new ArrayList<>();
        for (FieldDef f : schema.getFields()) {
            Type.Repetition rep = f.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
            if ("LONG".equals(f.getType()))    fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.INT64, f.getName()));
            else if ("INT".equals(f.getType())) fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.INT32, f.getName()));
            else if ("FLOAT".equals(f.getType())) fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.FLOAT, f.getName()));
            else if ("DOUBLE".equals(f.getType())) fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.DOUBLE, f.getName()));
            else if ("BOOLEAN".equals(f.getType())) fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.BOOLEAN, f.getName()));
            else fields.add(new PrimitiveType(rep, PrimitiveType.PrimitiveTypeName.BINARY, f.getName(), OriginalType.UTF8));
        }
        return new MessageType("record", fields);
    }

    @Override
    public void close() throws IOException { if (writer != null) writer.close(); }

    @Override
    public long getWrittenBytes() { return writer != null ? writer.getDataSize() : 0; }

    @Override
    public long getWrittenRows() { return writtenRows; }
}
```

- [ ] **Step 5: Implement CsvFileWriter**

```java
public class CsvFileWriter implements FormatWriter {
    private final StorageAdapter storage;
    private CSVPrinter printer;
    private OutputStream os;
    private long writtenRows = 0;
    private long writtenBytes = 0;
    private List<String> headers;

    public CsvFileWriter(StorageAdapter storage) { this.storage = storage; }

    @Override
    public void open(String filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException {
        this.headers = schema.getFields().stream().map(FieldDef::getName).collect(Collectors.toList());
        this.os = storage.create(filePath);
        // wrap with Gzip if needed
        OutputStream out = "GZIP".equals(config.getCompression())
            ? new java.util.zip.GZIPOutputStream(os) : os;
        this.printer = new CSVPrinter(new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8),
            CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])));
    }

    @Override
    public void writeRow(Object[] row) throws IOException {
        printer.printRecord(row);
        writtenRows++;
        for (Object o : row) { if (o != null) writtenBytes += o.toString().length(); }
    }

    @Override
    public void close() throws IOException { if (printer != null) printer.close(); }

    @Override public long getWrittenBytes() { return writtenBytes; }
    @Override public long getWrittenRows() { return writtenRows; }
}
```

- [ ] **Step 6: Implement FileWriterFactory**

```java
public class FileWriterFactory {
    private final StorageAdapter storage;
    public FileWriterFactory(StorageAdapter storage) { this.storage = storage; }

    public FormatWriter create(TopicSinkConfig config) {
        if ("CSV".equals(config.getSinkFormat())) return new CsvFileWriter(storage);
        return new ParquetFileWriter(storage);
    }
}
```

- [ ] **Step 7: Run all writer tests**

```bash
mvn test -pl lake-writer-worker -Dtest="ParquetFileWriterTest,CsvFileWriterTest"
```
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add Parquet and CSV format writers"
```

---

## Task 5: Worker — Schema & Path Resolver

**Files:**
- Create: `...schema/DynamicSchemaConverter.java`
- Create: `...schema/JsonRecordParser.java`
- Create: `...schema/PathResolver.java`
- Test: `...schema/PathResolverTest.java`
- Test: `...schema/JsonRecordParserTest.java`

- [ ] **Step 1: Write failing tests**

```java
// PathResolverTest.java
public class PathResolverTest {
    @Test
    public void testDateTemplate() {
        String resolved = PathResolver.resolve("/hdfs/data/{topic}/{date}", "orders", null);
        String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        assertEquals("/hdfs/data/orders/" + today, resolved);
    }

    @Test
    public void testDtFromMessage() {
        Map<String, String> msgFields = new HashMap<>();
        msgFields.put("dt", "2026-03-21");
        String resolved = PathResolver.resolve("/data/{topic}/dt={dt}/hour={hour}", "orders", msgFields);
        assertTrue(resolved.startsWith("/data/orders/dt=2026-03-21/hour="));
    }
}

// JsonRecordParserTest.java
public class JsonRecordParserTest {
    @Test
    public void testParseBatch() {
        SchemaDefinition schema = new SchemaDefinition();
        schema.setFields(Arrays.asList(
            new FieldDef("id", "LONG", false),
            new FieldDef("name", "STRING", true),
            new FieldDef("score", "DOUBLE", true)
        ));
        String json = "{\"id\":1,\"name\":\"alice\",\"score\":9.5}";
        JsonRecordParser parser = new JsonRecordParser();
        Object[] row = parser.parseToRow(json, schema.getFields().toArray(new FieldDef[0]));
        assertNotNull(row);
        assertEquals(1L, row[0]);
        assertEquals("alice", row[1]);
        assertEquals(9.5, (Double) row[2], 0.001);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl lake-writer-worker -Dtest="PathResolverTest,JsonRecordParserTest"
```
Expected: FAIL

- [ ] **Step 3: Implement PathResolver**

```java
public class PathResolver {
    private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat HOUR_FMT = new SimpleDateFormat("HH");

    public static String resolve(String template, String topic, Map<String, String> msgFields) {
        String today = DATE_FMT.format(new Date());
        String currentHour = HOUR_FMT.format(new Date());
        String result = template;
        result = result.replace("{topic}", topic);
        result = result.replace("{date}", today);
        if (msgFields != null) {
            result = result.replace("{dt}", msgFields.getOrDefault("dt", today));
            result = result.replace("{hour}", msgFields.getOrDefault("hour", currentHour));
        } else {
            result = result.replace("{dt}", today);
            result = result.replace("{hour}", currentHour);
        }
        return result;
    }

    /** Preview for UI — always uses system date/time */
    public static String preview(String template, String topic) {
        return resolve(template, topic, null);
    }
}
```

- [ ] **Step 4: Implement JsonRecordParser**

```java
public class JsonRecordParser {
    public Object[] parseToRow(String json, FieldDef[] schema) {
        try {
            JSONObject obj = JSON.parseObject(json);
            Object[] row = new Object[schema.length];
            for (int i = 0; i < schema.length; i++) {
                Object val = obj.get(schema[i].getName());
                row[i] = castValue(val, schema[i].getType());
            }
            return row;
        } catch (Exception e) {
            return null;
        }
    }

    // JDK 8: if-else instead of switch expression
    private Object castValue(Object val, String type) {
        if (val == null) return null;
        if ("STRING".equals(type))  return val.toString();
        if ("LONG".equals(type))    return ((Number) val).longValue();
        if ("INT".equals(type))     return ((Number) val).intValue();
        if ("DOUBLE".equals(type))  return ((Number) val).doubleValue();
        if ("FLOAT".equals(type))   return ((Number) val).floatValue();
        if ("BOOLEAN".equals(type)) return (Boolean) val;
        return val.toString();
    }

    public List<Object[]> parseBatch(List<String> jsonMessages, FieldDef[] schema) {
        List<Object[]> result = new ArrayList<>(jsonMessages.size());
        for (String json : jsonMessages) {
            Object[] row = parseToRow(json, schema);
            if (row != null) result.add(row);
        }
        return result;
    }
}
```

- [ ] **Step 5: Implement DynamicSchemaConverter**

```java
// Converts SchemaDefinition to Parquet MessageType (reuse from ParquetFileWriter logic)
public class DynamicSchemaConverter {
    public MessageType toParquetSchema(SchemaDefinition schema, String topic) {
        // delegate to same logic as ParquetFileWriter.buildMessageType
        // extract shared static method here
    }
    public FieldDef[] toFieldArray(SchemaDefinition schema) {
        return schema.getFields().toArray(new FieldDef[0]);
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest="PathResolverTest,JsonRecordParserTest"
```
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add JSON parser, path resolver, schema converter"
```

---

## Task 6: Worker — Write Buffer

**Files:**
- Create: `...buffer/WriteBuffer.java`
- Create: `...buffer/DoubleWriteBuffer.java`
- Create: `...buffer/WriteBufferManager.java`
- Create: `...buffer/BackpressureController.java`
- Test: `...buffer/WriteBufferManagerTest.java`

- [ ] **Step 1: Write failing tests**

```java
// WriteBufferManagerTest.java
public class WriteBufferManagerTest {
    @Test
    public void testCreateAndGetBuffer() {
        WriteBufferManager mgr = new WriteBufferManager(3L * 1024 * 1024 * 1024);
        TopicSinkConfig config = new TopicSinkConfig();
        config.setFlushRows(100);
        TopicPartition tp = new TopicPartition("orders", 0);
        mgr.createBuffer(tp, config);
        assertNotNull(mgr.getBuffer(tp));
    }

    @Test
    public void testBackpressure() {
        BackpressureController bp = new BackpressureController(100);
        assertTrue(bp.tryAcquire(50));
        assertTrue(bp.tryAcquire(40));
        assertFalse(bp.tryAcquire(20));  // would exceed 100
        bp.release(40);
        assertTrue(bp.tryAcquire(20));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl lake-writer-worker -Dtest=WriteBufferManagerTest
```

- [ ] **Step 3: Implement WriteBuffer**

```java
public class WriteBuffer {
    private final TopicPartition topicPartition;
    private final TopicSinkConfig config;
    private final List<Object[]> rows = new ArrayList<>();
    private long startOffset = -1;
    private long lastOffset = -1;
    private long totalBytes = 0;
    private final long createdAt = System.currentTimeMillis();

    public WriteBuffer(TopicPartition topicPartition, TopicSinkConfig config) {
        this.topicPartition = topicPartition;
        this.config = config;
    }

    public synchronized void append(Object[] row, long offset, long estimatedBytes) {
        rows.add(row);
        if (startOffset == -1) startOffset = offset;
        lastOffset = offset;
        totalBytes += estimatedBytes;
    }

    public synchronized boolean shouldFlush() {
        if (rows.isEmpty()) return false;
        long ageMs = System.currentTimeMillis() - createdAt;
        return rows.size() >= config.getFlushRows()
            || totalBytes >= config.getFlushBytes()
            || ageMs >= config.getFlushIntervalSec() * 1000L;
    }

    public synchronized List<Object[]> drainRows() { return new ArrayList<>(rows); }
    public boolean isEmpty() { return rows.isEmpty(); }
    public long getStartOffset() { return startOffset; }
    public long getLastOffset() { return lastOffset; }
    public long getTotalBytes() { return totalBytes; }
    public int getRowCount() { return rows.size(); }
    public TopicPartition getTopicPartition() { return topicPartition; }
    public TopicSinkConfig getConfig() { return config; }

    public synchronized void clear() {
        rows.clear();
        startOffset = -1;
        lastOffset = -1;
        totalBytes = 0;
    }
}
```

- [ ] **Step 4: Implement BackpressureController**

```java
public class BackpressureController {
    private final long maxBytes;
    private final AtomicLong currentBytes = new AtomicLong(0);

    public BackpressureController(long maxBytes) { this.maxBytes = maxBytes; }

    public boolean tryAcquire(long bytes) {
        long next = currentBytes.addAndGet(bytes);
        if (next > maxBytes) {
            currentBytes.addAndGet(-bytes);
            return false;
        }
        return true;
    }

    public void release(long bytes) { currentBytes.addAndGet(-bytes); }
    public boolean canConsume() { return currentBytes.get() < maxBytes * 0.9; }
}
```

- [ ] **Step 5: Implement WriteBufferManager and DoubleWriteBuffer**

```java
public class WriteBufferManager {
    private final ConcurrentHashMap<TopicPartition, DoubleWriteBuffer> buffers = new ConcurrentHashMap<>();
    private final BackpressureController backpressure;

    public WriteBufferManager(long maxTotalBytes) {
        this.backpressure = new BackpressureController(maxTotalBytes);
    }

    public void createBuffer(TopicPartition tp, TopicSinkConfig config) {
        buffers.put(tp, new DoubleWriteBuffer(tp, config));
    }

    public DoubleWriteBuffer getBuffer(TopicPartition tp) { return buffers.get(tp); }

    public void removeBuffer(TopicPartition tp) { buffers.remove(tp); }

    public List<DoubleWriteBuffer> getBuffersByTopic(String topic) {
        return buffers.entrySet().stream()
            .filter(e -> e.getKey().topic().equals(topic))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }

    public long getTotalBufferBytes() {
        return buffers.values().stream().mapToLong(b -> b.getActiveBuffer().getTotalBytes()).sum();
    }

    public BackpressureController getBackpressure() { return backpressure; }
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest=WriteBufferManagerTest
```
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add WriteBuffer, DoubleWriteBuffer, BackpressureController"
```

---

## Task 7: Worker — Checkpoint & Crash Recovery

**Files:**
- Create: `...checkpoint/Checkpoint.java`
- Create: `...checkpoint/CheckpointManager.java`
- Create: `...checkpoint/CrashRecoveryManager.java`
- Create: `...checkpoint/IdempotentWriteChecker.java`
- Test: `...checkpoint/CrashRecoveryManagerTest.java`

- [ ] **Step 1: Write failing test**

```java
// CrashRecoveryManagerTest.java
public class CrashRecoveryManagerTest {
    @Test
    public void testRecoverWhenTargetFileExists() throws Exception {
        File ckptDir = Files.createTempDirectory("ckpt").toFile();
        StorageAdapter mockStorage = mock(StorageAdapter.class);
        when(mockStorage.exists(anyString())).thenReturn(true);  // target exists

        Checkpoint ckpt = new Checkpoint("orders", 0, 1000L, 1999L, 1000,
            "/tmp/_tmp/part.tmp", "/tmp/part.parquet", "node1");
        CheckpointManager mgr = new CheckpointManager(ckptDir.getAbsolutePath());
        mgr.save(ckpt);

        CrashRecoveryManager recovery = new CrashRecoveryManager(ckptDir.getAbsolutePath(), mockStorage);
        Map<TopicPartition, Long> seekMap = recovery.recover();

        assertEquals(2000L, (long) seekMap.get(new TopicPartition("orders", 0)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=CrashRecoveryManagerTest
```

- [ ] **Step 3: Implement Checkpoint**

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Checkpoint {
    private String topic;
    private int partition;
    private long startOffset;
    private long endOffset;
    private int recordCount;
    private String tmpFilePath;
    private String targetFilePath;
    private String nodeId;
    private String createdAt;

    public String toJson() { return JSON.toJSONString(this); }

    public static Checkpoint fromJson(String json) {
        return JSON.parseObject(json, Checkpoint.class);
    }
}
```

- [ ] **Step 4: Implement CheckpointManager**

```java
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
    }

    public Optional<Checkpoint> load(TopicPartition tp) {
        File file = new File(new File(checkpointBaseDir, tp.topic()), tp.partition() + ".ckpt");
        if (!file.exists()) return Optional.empty();
        try {
            String json = new String(Files.readAllBytes(file.toPath()));
            return Optional.of(Checkpoint.fromJson(json));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public void delete(TopicPartition tp) {
        new File(new File(checkpointBaseDir, tp.topic()), tp.partition() + ".ckpt").delete();
    }

    public List<Checkpoint> loadAll() {
        List<Checkpoint> result = new ArrayList<>();
        File base = new File(checkpointBaseDir);
        if (!base.exists()) return result;
        for (File topicDir : base.listFiles(File::isDirectory)) {
            for (File f : topicDir.listFiles(f -> f.getName().endsWith(".ckpt"))) {
                try {
                    result.add(Checkpoint.fromJson(new String(Files.readAllBytes(f.toPath()))));
                } catch (IOException ignored) {}
            }
        }
        return result;
    }
}
```

- [ ] **Step 5: Implement CrashRecoveryManager**

```java
public class CrashRecoveryManager {
    private final CheckpointManager checkpointMgr;
    private final StorageAdapter storage;

    public CrashRecoveryManager(String checkpointDir, StorageAdapter storage) {
        this.checkpointMgr = new CheckpointManager(checkpointDir);
        this.storage = storage;
    }

    public Map<TopicPartition, Long> recover() {
        Map<TopicPartition, Long> seekOffsets = new HashMap<>();
        for (Checkpoint ckpt : checkpointMgr.loadAll()) {
            TopicPartition tp = new TopicPartition(ckpt.getTopic(), ckpt.getPartition());
            try {
                if (storage.exists(ckpt.getTargetFilePath())) {
                    // C-6/C-7: target exists, already done
                    seekOffsets.put(tp, ckpt.getEndOffset() + 1);
                } else if (ckpt.getTmpFilePath() != null && storage.exists(ckpt.getTmpFilePath())) {
                    // C-5: tmp exists, finish rename
                    storage.rename(ckpt.getTmpFilePath(), ckpt.getTargetFilePath());
                    seekOffsets.put(tp, ckpt.getEndOffset() + 1);
                } else {
                    // C-3/C-4: no file, roll back
                    seekOffsets.put(tp, ckpt.getStartOffset());
                }
            } catch (IOException e) {
                seekOffsets.put(tp, ckpt.getStartOffset());
            }
            checkpointMgr.delete(tp);
        }
        return seekOffsets;
    }

    public long recoverPartition(Checkpoint ckpt) {
        Map<TopicPartition, Long> m = recover();
        return m.getOrDefault(new TopicPartition(ckpt.getTopic(), ckpt.getPartition()), ckpt.getStartOffset());
    }
}
```

- [ ] **Step 6: Implement IdempotentWriteChecker**

```java
/**
 * Builds index of already-written offset ranges from existing filenames on HDFS.
 * Prevents duplicate writes in recovery/multi-node scenarios.
 * File name format: part-{nodeId}-P{partition}-{startOffset}to{endOffset}-{ts}.{ext}
 */
public class IdempotentWriteChecker {
    // key: TopicPartition, value: TreeMap<startOffset, endOffset>
    private final Map<TopicPartition, TreeMap<Long, Long>> writtenRanges = new ConcurrentHashMap<>();
    private final StorageAdapter storage;

    public IdempotentWriteChecker(StorageAdapter storage) { this.storage = storage; }

    /** Scan all files in sinkPath and build offset-range index from their names */
    public void buildIndex(String sinkPath, String topic) throws IOException {
        List<String> files = storage.listFiles(sinkPath);
        for (String filePath : files) {
            String name = filePath.substring(filePath.lastIndexOf('/') + 1);
            // pattern: part-nodeId-P{partition}-{start}to{end}-ts.compression.format
            java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("part-[^-]+-P(\\d+)-(\\d+)to(\\d+)-.*")
                .matcher(name);
            if (m.matches()) {
                int partition = Integer.parseInt(m.group(1));
                long startOffset = Long.parseLong(m.group(2));
                long endOffset   = Long.parseLong(m.group(3));
                TopicPartition tp = new TopicPartition(topic, partition);
                writtenRanges.computeIfAbsent(tp, k -> new TreeMap<>())
                             .put(startOffset, endOffset);
            }
        }
    }

    /** Returns true if the offset range [startOffset, endOffset] is fully covered */
    public boolean isAlreadyWritten(TopicPartition tp, long startOffset, long endOffset) {
        TreeMap<Long, Long> ranges = writtenRanges.get(tp);
        if (ranges == null) return false;
        Map.Entry<Long, Long> floor = ranges.floorEntry(startOffset);
        return floor != null && floor.getValue() >= endOffset;
    }
}
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest=CrashRecoveryManagerTest
```
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add Checkpoint, CrashRecoveryManager, IdempotentWriteChecker"
```

---

## Task 8: Worker — FlushExecutor (5-Phase Commit)

**Files:**
- Create: `...writer/FlushExecutor.java`
- Test: `...writer/FlushExecutorTest.java`

- [ ] **Step 1: Write failing test**

```java
public class FlushExecutorTest {
    @Test
    public void testSuccessfulFlush() throws Exception {
        StorageAdapter mockStorage = mock(StorageAdapter.class);
        when(mockStorage.create(any())).thenReturn(mock(OutputStream.class));
        when(mockStorage.rename(any(), any())).thenReturn(true);

        TopicSinkConfig config = new TopicSinkConfig();
        config.setSinkFormat("CSV");
        config.setCompression("NONE");
        config.setSinkPath("/data/orders/{date}");
        TopicPartition tp = new TopicPartition("orders", 0);
        WriteBuffer buffer = new WriteBuffer(tp, config);
        buffer.append(new Object[]{"val"}, 100L, 10L);

        SchemaDefinition schema = new SchemaDefinition();
        schema.setFields(Collections.singletonList(new FieldDef("f1", "STRING", true)));

        FlushExecutor executor = new FlushExecutor(mockStorage, new CheckpointManager("/tmp/ckpt-test"));
        FlushResult result = executor.flush(buffer, schema, "node1");
        assertTrue(result.isSuccess());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=FlushExecutorTest
```

- [ ] **Step 3: Implement FlushExecutor with 5-phase commit + retry**

```java
public class FlushExecutor {
    private static final int MAX_RETRIES = 3;
    private final StorageAdapter storage;
    private final CheckpointManager checkpointMgr;
    private final FileWriterFactory writerFactory;

    public FlushExecutor(StorageAdapter storage, CheckpointManager checkpointMgr) {
        this.storage = storage;
        this.checkpointMgr = checkpointMgr;
        this.writerFactory = new FileWriterFactory(storage);
    }

    public FlushResult flush(WriteBuffer buffer, SchemaDefinition schema, String nodeId) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                return doFlush(buffer, schema, nodeId);
            } catch (IOException e) {
                attempt++;
                if (attempt < MAX_RETRIES) {
                    sleepMs(2000L * (1L << (attempt - 1)));  // 2s, 4s
                }
            }
        }
        return FlushResult.failed("max retries exceeded");
    }

    private FlushResult doFlush(WriteBuffer buffer, SchemaDefinition schema, String nodeId) throws IOException {
        TopicPartition tp = buffer.getTopicPartition();
        TopicSinkConfig config = buffer.getConfig();
        String resolvedPath = PathResolver.resolve(config.getSinkPath(), tp.topic(), null);
        String ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String fileName = String.format("part-%s-P%d-%dto%d-%s.%s.%s",
            nodeId, tp.partition(), buffer.getStartOffset(), buffer.getLastOffset(),
            ts, config.getCompression().toLowerCase(), config.getSinkFormat().toLowerCase());
        String tmpPath = resolvedPath + "/_tmp/" + fileName.replace(
            "." + config.getSinkFormat().toLowerCase(), ".tmp");
        String targetPath = resolvedPath + "/" + fileName;

        // Phase 1: write tmp file
        List<Object[]> rows = buffer.drainRows();
        try (FormatWriter writer = writerFactory.create(config)) {
            writer.open(tmpPath, schema, config);
            writer.writeRows(rows);
        }

        // Phase 2: write checkpoint
        Checkpoint ckpt = new Checkpoint(tp.topic(), tp.partition(),
            buffer.getStartOffset(), buffer.getLastOffset(), rows.size(),
            tmpPath, targetPath, nodeId);
        checkpointMgr.save(ckpt);

        // Phase 3: rename (atomic on HDFS)
        storage.rename(tmpPath, targetPath);

        // Phase 4: commit offset is done by caller (KafkaConsumerPool)

        // Phase 5: delete checkpoint
        checkpointMgr.delete(tp);

        return FlushResult.success(targetPath, rows.size());
    }

    private void sleepMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest=FlushExecutorTest
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add FlushExecutor with 5-phase commit and retry"
```

---

## Task 9: Worker — Kafka Consumer

**Files:**
- Create: `...consumer/KafkaConsumerPool.java`
- Create: `...consumer/ConsumerWorker.java`
- Create: `...consumer/RecordDispatcher.java`
- Create: `...consumer/SafeRebalanceListener.java`
- Create: `...node/NodeIdentity.java`
- Test: `...consumer/KafkaConsumerPoolTest.java`

- [ ] **Step 1: Write failing test**

```java
public class KafkaConsumerPoolTest {
    @Test
    public void testNodeIdentityGeneratesUniqueId() {
        NodeIdentity n1 = new NodeIdentity();
        NodeIdentity n2 = new NodeIdentity();
        assertNotNull(n1.getNodeId());
        assertFalse(n1.getNodeId().isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=KafkaConsumerPoolTest
```

- [ ] **Step 3: Implement NodeIdentity**

```java
public class NodeIdentity {
    private final String nodeId;

    public NodeIdentity() {
        String id = System.getenv("POD_NAME");
        if (id == null || id.isEmpty()) id = System.getenv("HOSTNAME");
        if (id == null || id.isEmpty()) id = System.getenv("NODE_ID");
        if (id == null || id.isEmpty()) {
            try { id = InetAddress.getLocalHost().getHostName(); } catch (Exception ignored) {}
        }
        if (id == null || id.isEmpty()) id = "node-" + UUID.randomUUID().toString().substring(0, 8);
        this.nodeId = id;
    }

    public String getNodeId() { return nodeId; }
}
```

- [ ] **Step 4: Implement SafeRebalanceListener**

```java
public class SafeRebalanceListener implements ConsumerRebalanceListener {
    private final WriteBufferManager bufferManager;
    private final CheckpointManager checkpointMgr;
    private final CrashRecoveryManager recoveryMgr;
    private final KafkaConsumer<String, String> consumer;
    private final TopicMatcher topicMatcher;

    // ... (constructor omitted for brevity)

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            DoubleWriteBuffer buf = bufferManager.getBuffer(tp);
            if (buf != null && !buf.getActiveBuffer().isEmpty()) {
                try {
                    WriteBuffer toFlush = buf.swapForFlush();
                    TopicSinkConfig config = topicMatcher.match(tp.topic());
                    FlushResult result = flushExecutor.flush(toFlush,
                        config.parseSchema(), nodeIdentity.getNodeId());
                    if (result.isSuccess()) {
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        offsets.put(tp, new OffsetAndMetadata(toFlush.getLastOffset() + 1));
                        consumer.commitSync(offsets);
                    }
                } catch (Exception e) {
                    // offset not committed — new owner will re-consume
                }
            }
            bufferManager.removeBuffer(tp);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            Optional<Checkpoint> ckpt = checkpointMgr.load(tp);
            if (ckpt.isPresent()) {
                long seekTo = recoveryMgr.recoverPartition(ckpt.get());
                consumer.seek(tp, seekTo);
            }
            TopicSinkConfig config = topicMatcher.match(tp.topic());
            if (config != null) bufferManager.createBuffer(tp, config);
        }
    }
}
```

- [ ] **Step 5: Implement ConsumerWorker (poll loop)**

```java
public class ConsumerWorker implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final RecordDispatcher dispatcher;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final TopicMatcher topicMatcher;
    private final NodeIdentity nodeIdentity;
    private final BackpressureController backpressure;
    private volatile boolean running = true;

    @Override
    public void run() {
        while (running) {
            // backpressure check
            if (!backpressure.canConsume()) {
                try { Thread.sleep(100); } catch (InterruptedException e) { break; }
                continue;
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                dispatcher.dispatch(record);
            }
            // check flush triggers
            checkAndFlush();
        }
    }

    private void checkAndFlush() {
        for (TopicPartition tp : consumer.assignment()) {
            DoubleWriteBuffer buf = bufferManager.getBuffer(tp);
            if (buf != null && buf.getActiveBuffer().shouldFlush()) {
                WriteBuffer toFlush = buf.swapForFlush();
                TopicSinkConfig config = topicMatcher.match(tp.topic());
                flushExecutorPool.submit(() -> {
                    FlushResult result = flushExecutor.flush(toFlush, config.parseSchema(), nodeIdentity.getNodeId());
                    if (result.isSuccess()) {
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        offsets.put(tp, new OffsetAndMetadata(toFlush.getLastOffset() + 1));
                        consumer.commitSync(offsets);
                        backpressure.release(toFlush.getTotalBytes());
                        buf.recycleFlushed(toFlush);
                    }
                });
            }
        }
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-worker -Dtest=KafkaConsumerPoolTest
```
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add KafkaConsumer pool, SafeRebalanceListener, RecordDispatcher"
```

---

## Task 10: Worker — Config Poller, Heartbeat & Lifecycle

**Files:**
- Create: `...config/ConfigPoller.java`
- Create: `...config/ConfigDiff.java`
- Create: `...config/TopicMatcher.java`
- Create: `...heartbeat/HeartbeatReporter.java`
- Create: `...lifecycle/GracefulShutdownHook.java`
- Create: `...metrics/WriterMetrics.java`
- Create: `...worker/WorkerApplication.java`
- Create: `resources/application.yml`
- Test: `...config/ConfigPollerTest.java`

- [ ] **Step 1: Write failing test**

```java
public class ConfigPollerTest {
    @Test
    public void testConfigDiffDetectsAddedAndRemoved() {
        TopicSinkConfig oldCfg = new TopicSinkConfig();
        oldCfg.setTopicName("orders"); oldCfg.setVersion(1);
        TopicSinkConfig newCfg1 = new TopicSinkConfig();
        newCfg1.setTopicName("orders"); newCfg1.setVersion(1);
        TopicSinkConfig newCfg2 = new TopicSinkConfig();
        newCfg2.setTopicName("payments"); newCfg2.setVersion(1);

        Map<String, TopicSinkConfig> oldMap = Collections.singletonMap("orders", oldCfg);
        Map<String, TopicSinkConfig> newMap = new HashMap<>();
        newMap.put("orders", newCfg1);
        newMap.put("payments", newCfg2);

        ConfigDiff diff = ConfigDiff.compute(oldMap, newMap);
        assertEquals(1, diff.getAdded().size());
        assertEquals("payments", diff.getAdded().get(0).getTopicName());
        assertTrue(diff.getRemoved().isEmpty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl lake-writer-worker -Dtest=ConfigPollerTest
```

- [ ] **Step 3: Implement ConfigDiff**

```java
@Data
public class ConfigDiff {
    private final List<TopicSinkConfig> added;
    private final List<TopicSinkConfig> removed;
    private final List<TopicSinkConfig> schemaChanged;
    private final List<TopicSinkConfig> otherChanged;

    public boolean isEmpty() {
        return added.isEmpty() && removed.isEmpty() && schemaChanged.isEmpty() && otherChanged.isEmpty();
    }

    public static ConfigDiff compute(Map<String, TopicSinkConfig> oldMap, Map<String, TopicSinkConfig> newMap) {
        List<TopicSinkConfig> added = new ArrayList<>();
        List<TopicSinkConfig> removed = new ArrayList<>();
        List<TopicSinkConfig> schemaChanged = new ArrayList<>();
        List<TopicSinkConfig> otherChanged = new ArrayList<>();

        for (Map.Entry<String, TopicSinkConfig> e : newMap.entrySet()) {
            if (!oldMap.containsKey(e.getKey())) added.add(e.getValue());
        }
        for (Map.Entry<String, TopicSinkConfig> e : oldMap.entrySet()) {
            if (!newMap.containsKey(e.getKey())) removed.add(e.getValue());
        }
        for (Map.Entry<String, TopicSinkConfig> e : newMap.entrySet()) {
            TopicSinkConfig old = oldMap.get(e.getKey());
            if (old != null && old.getVersion() != e.getValue().getVersion()) {
                if (!old.getSchemaJson().equals(e.getValue().getSchemaJson())) {
                    schemaChanged.add(e.getValue());
                } else {
                    otherChanged.add(e.getValue());
                }
            }
        }
        return new ConfigDiff(added, removed, schemaChanged, otherChanged);
    }
}
```

- [ ] **Step 4: Implement TopicMatcher**

```java
public class TopicMatcher {
    private Map<String, TopicSinkConfig> exactConfigs = new HashMap<>();
    private Map<Pattern, TopicSinkConfig> regexConfigs = new LinkedHashMap<>();

    public synchronized void reload(List<TopicSinkConfig> configs) {
        exactConfigs.clear();
        regexConfigs.clear();
        for (TopicSinkConfig c : configs) {
            if ("EXACT".equals(c.getMatchType())) {
                exactConfigs.put(c.getTopicName(), c);
            } else {
                regexConfigs.put(Pattern.compile(c.getTopicName()), c);
            }
        }
    }

    public TopicSinkConfig match(String topic) {
        TopicSinkConfig c = exactConfigs.get(topic);
        if (c != null) return c;
        for (Map.Entry<Pattern, TopicSinkConfig> e : regexConfigs.entrySet()) {
            if (e.getKey().matcher(topic).matches()) return e.getValue();
        }
        return null;
    }

    public Set<String> resolveSubscriptionTopics(Set<String> allTopics) {
        Set<String> result = new HashSet<>(exactConfigs.keySet());
        for (Map.Entry<Pattern, TopicSinkConfig> e : regexConfigs.entrySet()) {
            for (String t : allTopics) {
                if (e.getKey().matcher(t).matches()) result.add(t);
            }
        }
        return result;
    }
}
```

- [ ] **Step 5: Implement ConfigPoller (Spring @Scheduled)**

```java
@Component
public class ConfigPoller {
    private volatile Map<String, TopicSinkConfig> currentConfigMap = new ConcurrentHashMap<>();
    private final TopicSinkConfigRepository configRepo;
    private final TopicMatcher topicMatcher;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;

    @PostConstruct
    public void init() { syncConfig(); }

    @Scheduled(fixedDelay = 30000)
    public void syncConfig() {
        Map<String, TopicSinkConfig> latest = configRepo.findAllActive().stream()
            .collect(Collectors.toMap(TopicSinkConfig::getTopicName, c -> c));
        ConfigDiff diff = ConfigDiff.compute(currentConfigMap, latest);
        if (!diff.isEmpty()) {
            topicMatcher.reload(new ArrayList<>(latest.values()));
            handleSchemaChanged(diff.getSchemaChanged());
            // added/removed handled via Consumer.subscribe update + rebalance
        }
        currentConfigMap = new ConcurrentHashMap<>(latest);
    }

    private void handleSchemaChanged(List<TopicSinkConfig> changed) {
        for (TopicSinkConfig newConfig : changed) {
            // flush existing buffers with old schema, recreate with new schema
            bufferManager.getBuffersByTopic(newConfig.getTopicName()).forEach(buf -> {
                WriteBuffer toFlush = buf.swapForFlush();
                if (!toFlush.isEmpty()) {
                    TopicSinkConfig old = currentConfigMap.get(newConfig.getTopicName());
                    flushExecutor.flush(toFlush, old.parseSchema(), "node");
                }
                bufferManager.createBuffer(toFlush.getTopicPartition(), newConfig);
            });
        }
    }
}
```

- [ ] **Step 6: Implement HeartbeatReporter**

```java
@Component
public class HeartbeatReporter {
    private final WorkerHeartbeatRepository heartbeatRepo;
    private final WriteBufferManager bufferManager;
    private final NodeIdentity nodeIdentity;
    private final long startTime = System.currentTimeMillis();

    @Scheduled(fixedDelay = 15000)
    public void report() {
        WorkerHeartbeat hb = new WorkerHeartbeat();
        hb.setNodeId(nodeIdentity.getNodeId());
        hb.setStatus("ONLINE");
        hb.setBufferUsageBytes(bufferManager.getTotalBufferBytes());
        hb.setUptimeSec((System.currentTimeMillis() - startTime) / 1000);
        hb.setLastHeartbeat(new Date());
        heartbeatRepo.save(hb);
    }
}
```

- [ ] **Step 7: Implement GracefulShutdownHook**

```java
@Component
public class GracefulShutdownHook {
    private final KafkaConsumerPool consumerPool;
    private final WriteBufferManager bufferManager;
    private final FlushExecutor flushExecutor;
    private final StorageAdapter storage;

    @PreDestroy
    public void shutdown() {
        consumerPool.stopPolling();
        // flush all buffers
        bufferManager.getBuffersByTopic("*").forEach(buf -> {
            WriteBuffer toFlush = buf.swapForFlush();
            if (!toFlush.isEmpty()) {
                TopicSinkConfig config = toFlush.getConfig();
                flushExecutor.flush(toFlush, config.parseSchema(), "shutdown");
            }
        });
        consumerPool.close();
        try { storage.close(); } catch (Exception ignored) {}
    }
}
```

- [ ] **Step 8: Create application.yml for Worker**

```yaml
# lake-writer-worker/src/main/resources/application.yml
server:
  port: 8081
spring:
  application:
    name: lake-writer-worker
  main:
    web-application-type: none
  datasource:
    url: jdbc:mysql://${DB_HOST:localhost}:3306/lake_writer?useSSL=false&serverTimezone=Asia/Shanghai
    username: ${DB_USERNAME:root}
    password: ${DB_PASSWORD:root}
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 3
lake-writer:
  kafka:
    bootstrap-servers: ${KAFKA_SERVERS:localhost:9092}
    group-id: ${GROUP_ID:kafka-lake-writer}
    consumer-count: ${CONSUMER_COUNT:4}
  storage:
    type: hdfs
    hdfs:
      default-fs: ${HDFS_URL:hdfs://namenode:8020}
  buffer:
    max-total-bytes: 3221225472
  checkpoint:
    dir: ${CHECKPOINT_DIR:./checkpoint}
  node:
    id: ${POD_NAME:${HOSTNAME:}}
management:
  server:
    port: 8081
  endpoints:
    web:
      exposure:
        include: health,prometheus
```

- [ ] **Step 9: Implement VersionedConfigSync (multi-node rebalance storm mitigation)**

```java
/**
 * Delays subscription changes until a grace period has passed since config update,
 * allowing all nodes to see the same config version before re-subscribing.
 * This reduces the number of Kafka Rebalances from N (one per node) to ~1.
 */
public class VersionedConfigSync {
    private static final long GRACE_PERIOD_MS = 60_000L;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Consumer<TopicSinkConfig> applyFn;

    public VersionedConfigSync(Consumer<TopicSinkConfig> applySubscriptionChange) {
        this.applyFn = applySubscriptionChange;
    }

    /**
     * Called when ConfigPoller detects an added/removed topic.
     * Schedules subscription change after grace period from config.updatedAt.
     */
    public void onConfigChanged(TopicSinkConfig newConfig) {
        long updatedAtMs = newConfig.getUpdatedAt() != null
            ? newConfig.getUpdatedAt().getTime() : System.currentTimeMillis();
        long effectiveTime = updatedAtMs + GRACE_PERIOD_MS;
        long delayMs = Math.max(0, effectiveTime - System.currentTimeMillis());
        if (delayMs == 0) {
            applyFn.accept(newConfig);
        } else {
            scheduler.schedule(() -> applyFn.accept(newConfig), delayMs, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() { scheduler.shutdownNow(); }
}
```
Wire into ConfigPoller: wrap `handleTopicAdded()` and `handleTopicRemoved()` calls through `VersionedConfigSync.onConfigChanged()`.

- [ ] **Step 10: Run config tests**

```bash
mvn test -pl lake-writer-worker -Dtest=ConfigPollerTest
```
Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add lake-writer-worker/src/
git commit -m "feat(worker): add ConfigPoller, TopicMatcher, HeartbeatReporter, VersionedConfigSync, GracefulShutdown"
```

---

## Task 11: Admin — REST API & Services

**Files:**
- Create: `...admin/AdminApplication.java`
- Create: `...admin/controller/ConfigController.java`
- Create: `...admin/controller/StatusController.java`
- Create: `...admin/controller/ChangeLogController.java`
- Create: `...admin/service/ConfigService.java`
- Create: `...admin/service/ConfigValidator.java`
- Create: `...admin/service/StatusAggregator.java`
- Create: `...admin/config/WebMvcConfig.java`
- Create: `resources/application.yml`
- Test: `...admin/controller/ConfigControllerTest.java`

- [ ] **Step 1: Write failing tests**

```java
@SpringBootTest
@AutoConfigureMockMvc
public class ConfigControllerTest {
    @Autowired private MockMvc mockMvc;

    @Test
    public void testGetConfigsReturns200() throws Exception {
        mockMvc.perform(get("/api/v1/configs"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.code").value(200));
    }

    @Test
    public void testValidateConfig() throws Exception {
        String body = "{\"sinkPath\":\"/hdfs/data/{date}\",\"schemaJson\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"LONG\\\"}]}\"}";
        mockMvc.perform(post("/api/v1/configs/validate")
                .contentType("application/json").content(body))
            .andExpect(status().isOk());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl lake-writer-admin -Dtest=ConfigControllerTest
```

- [ ] **Step 3: Implement AdminApplication**

```java
@SpringBootApplication
@EnableScheduling
public class AdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}
```

- [ ] **Step 4: Implement ConfigService**

```java
@Service
@Transactional
public class ConfigService {
    private final TopicSinkConfigRepository configRepo;
    private final ConfigChangeLogRepository changeLogRepo;

    public Page<TopicSinkConfig> findAll(Pageable pageable, String status) {
        if (status != null) return configRepo.findByStatus(status, pageable);
        return configRepo.findAll(pageable);
    }

    public TopicSinkConfig create(TopicSinkConfig config) {
        config.setVersion(1);
        config.setCreatedAt(new Date());
        config.setUpdatedAt(new Date());
        TopicSinkConfig saved = configRepo.save(config);
        writeChangeLog(saved, null, saved, "CREATED", "admin");
        return saved;
    }

    public TopicSinkConfig update(Long id, TopicSinkConfig update) {
        TopicSinkConfig existing = configRepo.findById(id).orElseThrow();
        String before = JSON.toJSONString(existing);
        existing.setSchemaJson(update.getSchemaJson());
        existing.setSinkPath(update.getSinkPath());
        existing.setSinkFormat(update.getSinkFormat());
        existing.setCompression(update.getCompression());
        existing.setFlushRows(update.getFlushRows());
        existing.setFlushBytes(update.getFlushBytes());
        existing.setFlushIntervalSec(update.getFlushIntervalSec());
        existing.setDescription(update.getDescription());
        existing.setVersion(existing.getVersion() + 1);
        existing.setUpdatedAt(new Date());
        TopicSinkConfig saved = configRepo.save(existing);
        writeChangeLog(saved, before, JSON.toJSONString(saved), "UPDATED", "admin");
        return saved;
    }

    public void pause(Long id) {
        TopicSinkConfig c = configRepo.findById(id).orElseThrow();
        c.setStatus("PAUSED"); c.setVersion(c.getVersion() + 1); c.setUpdatedAt(new Date());
        configRepo.save(c);
        writeChangeLog(c, null, null, "PAUSED", "admin");
    }

    public void resume(Long id) {
        TopicSinkConfig c = configRepo.findById(id).orElseThrow();
        c.setStatus("ACTIVE"); c.setVersion(c.getVersion() + 1); c.setUpdatedAt(new Date());
        configRepo.save(c);
        writeChangeLog(c, null, null, "RESUMED", "admin");
    }

    private void writeChangeLog(TopicSinkConfig config, String before, String after,
                                 String type, String operator) {
        ConfigChangeLog log = new ConfigChangeLog();
        log.setConfigId(config.getId());
        log.setTopicName(config.getTopicName());
        log.setChangeType(type);
        log.setOperator(operator);
        log.setBeforeJson(before);
        log.setAfterJson(after);
        log.setCreatedAt(new Date());
        changeLogRepo.save(log);
    }
}
```

- [ ] **Step 5: Implement ConfigController**

```java
@RestController
@RequestMapping("/api/v1/configs")
public class ConfigController {
    private final ConfigService configService;
    private final ConfigValidator validator;

    @GetMapping
    public ResponseEntity<?> list(@RequestParam(required=false) String status,
                                   @PageableDefault Pageable pageable) {
        return ok(configService.findAll(pageable, status));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable Long id) {
        return ok(configService.findById(id));
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody TopicSinkConfig config) {
        ValidationResult v = validator.validate(config);
        if (!v.isValid()) return badRequest(v.getErrors());
        return ok(configService.create(config));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @RequestBody TopicSinkConfig config) {
        return ok(configService.update(id, config));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        configService.delete(id);
        return ok("deleted");
    }

    @PutMapping("/{id}/pause")
    public ResponseEntity<?> pause(@PathVariable Long id) {
        configService.pause(id); return ok("paused");
    }

    @PutMapping("/{id}/resume")
    public ResponseEntity<?> resume(@PathVariable Long id) {
        configService.resume(id); return ok("resumed");
    }

    @PostMapping("/validate")
    public ResponseEntity<?> validate(@RequestBody Map<String, String> req) {
        return ok(validator.validatePathAndSchema(req.get("sinkPath"), req.get("schemaJson")));
    }

    @GetMapping("/{id}/changelog")
    public ResponseEntity<?> changelog(@PathVariable Long id) {
        return ok(changeLogRepo.findByConfigIdOrderByCreatedAtDesc(id));
    }

    private ResponseEntity<?> ok(Object data) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 200); r.put("data", data);
        return ResponseEntity.ok(r);
    }

    private ResponseEntity<?> badRequest(Object errors) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 400); r.put("errors", errors);
        return ResponseEntity.badRequest().body(r);
    }
}
```

- [ ] **Step 6: Implement StatusAggregator and StatusController**

```java
@Service
public class StatusAggregator {
    private final WorkerHeartbeatRepository heartbeatRepo;

    public List<Map<String, Object>> aggregateAll() {
        Date threshold = new Date(System.currentTimeMillis() - 60_000);
        return heartbeatRepo.findAll().stream().map(hb -> {
            Map<String, Object> ws = new HashMap<>();
            ws.put("nodeId", hb.getNodeId());
            ws.put("online", hb.getLastHeartbeat() != null && hb.getLastHeartbeat().after(threshold));
            ws.put("recordsPerSec", hb.getRecordsPerSec());
            ws.put("bufferUsageBytes", hb.getBufferUsageBytes());
            ws.put("lastHeartbeat", hb.getLastHeartbeat());
            ws.put("assignedPartitions", hb.getAssignedPartitions());
            return ws;
        }).collect(Collectors.toList());
    }
}

@RestController
@RequestMapping("/api/v1/status")
public class StatusController {
    private final StatusAggregator aggregator;

    @GetMapping("/nodes")
    public ResponseEntity<?> nodes() {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 200); r.put("data", aggregator.aggregateAll());
        return ResponseEntity.ok(r);
    }
}
```

- [ ] **Step 7: Create application.yml for Admin**

```yaml
# lake-writer-admin/src/main/resources/application.yml
server:
  port: 8080
spring:
  application:
    name: lake-writer-admin
  datasource:
    url: jdbc:mysql://${DB_HOST:localhost}:3306/lake_writer?useSSL=false&serverTimezone=Asia/Shanghai
    username: ${DB_USERNAME:root}
    password: ${DB_PASSWORD:root}
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 10
  resources:
    static-locations: classpath:/static/
management:
  endpoints:
    web:
      exposure:
        include: health,info
```

- [ ] **Step 8: Run tests to verify they pass**

```bash
mvn test -pl lake-writer-admin -Dtest=ConfigControllerTest
```
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add lake-writer-admin/src/
git commit -m "feat(admin): add REST API controllers and services"
```

---

## Task 12: Admin — Vue.js Frontend

**Files:**
- Create: `lake-writer-admin/frontend/package.json`
- Create: `lake-writer-admin/frontend/src/main.js`
- Create: `lake-writer-admin/frontend/src/router/index.js`
- Create: `lake-writer-admin/frontend/src/api/config.js`
- Create: `lake-writer-admin/frontend/src/api/status.js`
- Create: `lake-writer-admin/frontend/src/views/Dashboard.vue`
- Create: `lake-writer-admin/frontend/src/views/ConfigList.vue`
- Create: `lake-writer-admin/frontend/src/views/ConfigEdit.vue`
- Create: `lake-writer-admin/frontend/src/views/NodeList.vue`
- Create: `lake-writer-admin/frontend/src/views/ChangeLog.vue`
- Create: `lake-writer-admin/frontend/src/components/SchemaEditor.vue`
- Create: `lake-writer-admin/frontend/src/components/PathPreview.vue`

- [ ] **Step 1: Initialize Vue project**

```bash
cd lake-writer-admin/frontend
npm init -y
npm install vue@2 vue-router@3 element-ui axios echarts@4
npm install -D @vue/cli-service vue-template-compiler
```

- [ ] **Step 2: Create package.json scripts**

```json
{
  "scripts": {
    "serve": "vue-cli-service serve",
    "build": "vue-cli-service build --dest ../src/main/resources/static"
  }
}
```

- [ ] **Step 3: Create main.js**

```javascript
import Vue from 'vue'
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'
import App from './App.vue'
import router from './router'

Vue.use(ElementUI)
Vue.config.productionTip = false

new Vue({ router, render: h => h(App) }).$mount('#app')
```

- [ ] **Step 4: Create router/index.js**

```javascript
import Vue from 'vue'
import Router from 'vue-router'
import Dashboard from '../views/Dashboard.vue'
import ConfigList from '../views/ConfigList.vue'
import ConfigEdit from '../views/ConfigEdit.vue'
import NodeList from '../views/NodeList.vue'
import ChangeLog from '../views/ChangeLog.vue'

Vue.use(Router)

export default new Router({
  mode: 'history',
  routes: [
    { path: '/', component: Dashboard },
    { path: '/configs', component: ConfigList },
    { path: '/configs/new', component: ConfigEdit },
    { path: '/configs/:id/edit', component: ConfigEdit, props: true },
    { path: '/nodes', component: NodeList },
    { path: '/changelog', component: ChangeLog }
  ]
})
```

- [ ] **Step 5: Create API modules**

```javascript
// api/config.js
import axios from 'axios'
const BASE = '/api/v1/configs'

export const getConfigs = (params) => axios.get(BASE, { params })
export const getConfig = (id) => axios.get(`${BASE}/${id}`)
export const createConfig = (data) => axios.post(BASE, data)
export const updateConfig = (id, data) => axios.put(`${BASE}/${id}`, data)
export const deleteConfig = (id) => axios.delete(`${BASE}/${id}`)
export const pauseConfig = (id) => axios.put(`${BASE}/${id}/pause`)
export const resumeConfig = (id) => axios.put(`${BASE}/${id}/resume`)
export const validateConfig = (data) => axios.post(`${BASE}/validate`, data)
export const getChangelog = (id) => axios.get(`${BASE}/${id}/changelog`)

// api/status.js
export const getNodes = () => axios.get('/api/v1/status/nodes')
```

- [ ] **Step 6: Create SchemaEditor component**

```vue
<!-- components/SchemaEditor.vue -->
<template>
  <div>
    <el-table :data="fields" border size="small">
      <el-table-column label="字段名" prop="name">
        <template slot-scope="{row}">
          <el-input v-model="row.name" size="mini" />
        </template>
      </el-table-column>
      <el-table-column label="类型" width="120">
        <template slot-scope="{row}">
          <el-select v-model="row.type" size="mini">
            <el-option v-for="t in types" :key="t" :label="t" :value="t" />
          </el-select>
        </template>
      </el-table-column>
      <el-table-column label="可为空" width="80">
        <template slot-scope="{row}">
          <el-checkbox v-model="row.nullable" />
        </template>
      </el-table-column>
      <el-table-column label="操作" width="80">
        <template slot-scope="{$index}">
          <el-button type="danger" icon="el-icon-delete" size="mini" circle @click="remove($index)" />
        </template>
      </el-table-column>
    </el-table>
    <el-button type="primary" size="small" icon="el-icon-plus" style="margin-top:8px" @click="add">添加字段</el-button>
  </div>
</template>
<script>
export default {
  props: { value: Array },
  data() {
    return {
      fields: this.value || [],
      types: ['STRING', 'LONG', 'INT', 'DOUBLE', 'FLOAT', 'BOOLEAN']
    }
  },
  methods: {
    add() { this.fields.push({ name: '', type: 'STRING', nullable: true }) },
    remove(i) { this.fields.splice(i, 1) }
  },
  watch: {
    fields: { deep: true, handler(v) { this.$emit('input', v) } }
  }
}
</script>
```

- [ ] **Step 7: Create PathPreview component**

```vue
<!-- components/PathPreview.vue -->
<template>
  <el-tag type="info" v-if="preview">预览: {{ preview }}</el-tag>
</template>
<script>
export default {
  props: { template: String, topic: String },
  computed: {
    preview() {
      if (!this.template) return ''
      const today = new Date().toISOString().slice(0, 10)
      const hour = String(new Date().getHours()).padStart(2, '0')
      return this.template
        .replace('{date}', today)
        .replace('{dt}', today)
        .replace('{hour}', hour)
        .replace('{topic}', this.topic || 'topic')
    }
  }
}
</script>
```

- [ ] **Step 8: Create ConfigList.vue**

```vue
<template>
  <div>
    <div style="margin-bottom:16px">
      <el-button type="primary" @click="$router.push('/configs/new')">+ 新增配置</el-button>
      <el-select v-model="statusFilter" placeholder="状态过滤" clearable style="margin-left:8px">
        <el-option label="全部" value="" />
        <el-option label="活跃" value="ACTIVE" />
        <el-option label="暂停" value="PAUSED" />
      </el-select>
    </div>
    <el-table :data="configs" border v-loading="loading">
      <el-table-column label="Topic" prop="topicName" />
      <el-table-column label="匹配" prop="matchType" width="80" />
      <el-table-column label="格式" prop="sinkFormat" width="90" />
      <el-table-column label="路径(今日)">
        <template slot-scope="{row}">
          <PathPreview :template="row.sinkPath" :topic="row.topicName" />
        </template>
      </el-table-column>
      <el-table-column label="状态" width="90">
        <template slot-scope="{row}">
          <el-tag :type="row.status === 'ACTIVE' ? 'success' : 'info'">{{ row.status }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="200">
        <template slot-scope="{row}">
          <el-button size="mini" @click="edit(row.id)">编辑</el-button>
          <el-button size="mini" :type="row.status==='ACTIVE'?'warning':'success'"
                     @click="togglePause(row)">{{ row.status==='ACTIVE'?'暂停':'恢复' }}</el-button>
          <el-button size="mini" type="danger" @click="remove(row.id)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import { getConfigs, pauseConfig, resumeConfig, deleteConfig } from '@/api/config'
import PathPreview from '@/components/PathPreview'
export default {
  components: { PathPreview },
  data() { return { configs: [], loading: false, statusFilter: '' } },
  created() { this.load() },
  methods: {
    async load() {
      this.loading = true
      const r = await getConfigs({ status: this.statusFilter || undefined })
      this.configs = r.data.data.content || r.data.data
      this.loading = false
    },
    edit(id) { this.$router.push(`/configs/${id}/edit`) },
    async togglePause(row) {
      if (row.status === 'ACTIVE') await pauseConfig(row.id)
      else await resumeConfig(row.id)
      this.load()
    },
    async remove(id) {
      await this.$confirm('确认删除?', '提示', { type: 'warning' })
      await deleteConfig(id)
      this.load()
    }
  }
}
</script>
```

- [ ] **Step 9: Create ConfigEdit.vue**

```vue
<template>
  <el-card>
    <div slot="header">{{ id ? '编辑配置' : '新增配置' }}</div>
    <el-form :model="form" label-width="100px">
      <el-form-item label="Topic名称"><el-input v-model="form.topicName" /></el-form-item>
      <el-form-item label="匹配方式">
        <el-radio-group v-model="form.matchType">
          <el-radio label="EXACT">精确</el-radio>
          <el-radio label="REGEX">正则</el-radio>
        </el-radio-group>
      </el-form-item>
      <el-form-item label="写入格式">
        <el-select v-model="form.sinkFormat">
          <el-option label="Parquet" value="PARQUET" />
          <el-option label="CSV" value="CSV" />
        </el-select>
      </el-form-item>
      <el-form-item label="压缩算法">
        <el-select v-model="form.compression">
          <el-option v-for="c in ['SNAPPY','GZIP','NONE']" :key="c" :label="c" :value="c" />
        </el-select>
      </el-form-item>
      <el-form-item label="存储路径">
        <el-input v-model="form.sinkPath" placeholder="/hdfs/kafka/data/{topic}/{date}" />
        <PathPreview :template="form.sinkPath" :topic="form.topicName" />
      </el-form-item>
      <el-form-item label="Schema定义">
        <SchemaEditor v-model="schemaFields" />
      </el-form-item>
      <el-form-item label="Flush行数"><el-input-number v-model="form.flushRows" :min="1000" /></el-form-item>
      <el-form-item label="Flush间隔(秒)"><el-input-number v-model="form.flushIntervalSec" :min="60" /></el-form-item>
      <el-form-item label="描述"><el-input v-model="form.description" type="textarea" /></el-form-item>
      <el-form-item>
        <el-button type="primary" @click="save">保存配置</el-button>
        <el-button @click="$router.back()">取消</el-button>
      </el-form-item>
      <el-alert type="info" :closable="false" title="保存后将在30秒内自动生效，无需重启消费实例" />
    </el-form>
  </el-card>
</template>
<script>
import { getConfig, createConfig, updateConfig } from '@/api/config'
import SchemaEditor from '@/components/SchemaEditor'
import PathPreview from '@/components/PathPreview'
export default {
  components: { SchemaEditor, PathPreview },
  props: { id: [String, Number] },
  data() {
    return {
      form: { topicName: '', matchType: 'EXACT', sinkFormat: 'PARQUET',
              compression: 'SNAPPY', sinkPath: '', flushRows: 500000,
              flushIntervalSec: 600, description: '' },
      schemaFields: []
    }
  },
  async created() {
    if (this.id) {
      const r = await getConfig(this.id)
      this.form = r.data.data
      this.schemaFields = JSON.parse(this.form.schemaJson).fields || []
    }
  },
  methods: {
    async save() {
      this.form.schemaJson = JSON.stringify({ fields: this.schemaFields })
      if (this.id) await updateConfig(this.id, this.form)
      else await createConfig(this.form)
      this.$message.success('保存成功')
      this.$router.push('/configs')
    }
  }
}
</script>
```

- [ ] **Step 10: Create NodeList.vue and Dashboard.vue**

```vue
<!-- NodeList.vue -->
<template>
  <el-table :data="nodes" border v-loading="loading">
    <el-table-column label="节点ID" prop="nodeId" />
    <el-table-column label="状态" width="90">
      <template slot-scope="{row}">
        <el-tag :type="row.online ? 'success' : 'danger'">{{ row.online ? '在线' : '离线' }}</el-tag>
      </template>
    </el-table-column>
    <el-table-column label="消费速率(条/秒)" prop="recordsPerSec" />
    <el-table-column label="Buffer用量" :formatter="r => formatBytes(r.bufferUsageBytes)" />
    <el-table-column label="最后心跳" prop="lastHeartbeat" />
  </el-table>
</template>
<script>
import { getNodes } from '@/api/status'
export default {
  data() { return { nodes: [], loading: false } },
  created() { this.load() },
  methods: {
    async load() {
      this.loading = true
      const r = await getNodes()
      this.nodes = r.data.data
      this.loading = false
    },
    formatBytes(b) { return b > 1024*1024 ? (b/1024/1024).toFixed(1)+' MB' : (b/1024).toFixed(0)+' KB' }
  }
}
</script>
```

- [ ] **Step 11: Build frontend**

```bash
cd lake-writer-admin/frontend && npm run build
```
Expected: Static files output to `../src/main/resources/static/`

- [ ] **Step 12: Commit**

```bash
git add lake-writer-admin/frontend/ lake-writer-admin/src/main/resources/static/
git commit -m "feat(admin): add Vue.js frontend with config management and node monitoring"
```

---

## Task 13: Docker & Integration Verification

**Files:**
- Create: `docker/Dockerfile.worker`
- Create: `docker/Dockerfile.admin`
- Create: `docker/docker-compose.yml`

- [ ] **Step 1: Create Dockerfiles**

```dockerfile
# docker/Dockerfile.worker
FROM openjdk:8-jre-slim
WORKDIR /app
COPY lake-writer-worker/target/lake-writer-worker-*.jar app.jar
ENV JAVA_OPTS="-Xms5g -Xmx5g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
EXPOSE 8081
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
```

```dockerfile
# docker/Dockerfile.admin
FROM openjdk:8-jre-slim
WORKDIR /app
COPY lake-writer-admin/target/lake-writer-admin-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 2: Create docker-compose.yml for local dev**

```yaml
version: '3.8'
services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: lake_writer
    ports: ["3306:3306"]
    volumes:
      - ../sql/init.sql:/docker-entrypoint-initdb.d/init.sql

  zookeeper:
    image: confluentinc/cp-zookeeper:6.2.0
    environment: { ZOOKEEPER_CLIENT_PORT: 2181 }

  kafka:
    image: confluentinc/cp-kafka:6.2.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

  admin:
    build: { context: .., dockerfile: docker/Dockerfile.admin }
    ports: ["8080:8080"]
    environment:
      DB_HOST: mysql
    depends_on: [mysql]
```

- [ ] **Step 3: Run full build and verify**

```bash
cd kafka-lake-writer
mvn clean package -DskipTests
```
Expected: `BUILD SUCCESS`, jars in target/ for all modules

- [ ] **Step 4: Run all unit tests**

```bash
mvn test
```
Expected: All tests pass

- [ ] **Step 5: Final commit**

```bash
git add docker/
git commit -m "feat: add Docker build files and docker-compose for local dev"
```

---

## Summary: Implementation Order

| Priority | Task | Est. Complexity |
|----------|------|-----------------|
| 1 | Maven scaffolding + SQL | Low |
| 2 | Common entities + repos | Low |
| 3 | Storage adapter | Low |
| 4 | Format writers (Parquet/CSV) | Medium |
| 5 | Schema + Path resolver | Low |
| 6 | Write buffer + backpressure | Medium |
| 7 | Checkpoint + crash recovery | High |
| 8 | FlushExecutor (5-phase commit) | High |
| 9 | Kafka consumer pool | High |
| 10 | Config poller + heartbeat | Medium |
| 11 | Admin REST API | Medium |
| 12 | Vue.js frontend | Medium |
| 13 | Docker + integration | Low |

**Critical paths:**
- Tasks 3→4→8 (storage → writers → flush) must be sequential
- Tasks 6→7→8→9 (buffer → checkpoint → flush → consumer) must be sequential
- Tasks 1→2 must precede all others
- Tasks 11→12 are independent of Worker (Tasks 3-10)
