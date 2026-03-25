# Kafka-Lake-Writer 项目结构与模块设计

## 一、项目结构

```
kafka-lake-writer/                       # Maven多模块项目(parent pom)
├── pom.xml                              # Parent POM, 管理统一版本
├── docs/
│   └── design/                          # 设计文档
│       ├── 01-requirements.md
│       ├── ...
│       └── 08-project-structure.md
├── sql/
│   └── init.sql                         # 数据库初始化脚本(含心跳表)
├── docker/
│   ├── Dockerfile.worker                # Worker消费节点镜像
│   ├── Dockerfile.admin                 # Admin管理服务镜像
│   └── docker-compose.yml               # 本地开发环境
│
│── =====================================================================
│   lake-writer-common: 公共模块(实体类/DAO/工具, Admin和Worker共享)
│── =====================================================================
├── lake-writer-common/
│   ├── pom.xml
│   └── src/main/java/com/lakewriter/common/
│       ├── model/
│       │   ├── TopicSinkConfig.java      # 配置实体
│       │   ├── FieldDef.java             # Schema字段定义
│       │   ├── SchemaDefinition.java     # Schema定义(字段数组)
│       │   ├── ConfigChangeLog.java      # 配置变更审计实体
│       │   └── WorkerHeartbeat.java      # Worker心跳状态实体
│       └── repository/
│           ├── TopicSinkConfigRepository.java
│           ├── ConfigChangeLogRepository.java
│           └── WorkerHeartbeatRepository.java
│
│── =====================================================================
│   lake-writer-worker: 消费节点(纯消费写入, 无Web/无管理API)
│── =====================================================================
├── lake-writer-worker/
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/lakewriter/worker/
│       │   ├── WorkerApplication.java            # Spring Boot启动类(无Web)
│       │   │
│       │   ├── config/                           # 配置拉取(只读MySQL)
│       │   │   ├── ConfigPoller.java             # 定时轮询配置(只读)
│       │   │   ├── ConfigDiff.java               # 配置差异计算
│       │   │   └── TopicMatcher.java             # Topic匹配引擎
│       │   │
│       │   ├── heartbeat/                        # 心跳上报
│       │   │   └── HeartbeatReporter.java        # 每15秒上报状态到MySQL
│       │   │
│       │   ├── consumer/                         # Kafka消费
│       │   │   ├── KafkaConsumerPool.java
│       │   │   ├── ConsumerWorker.java
│       │   │   ├── RecordDispatcher.java
│       │   │   └── SafeRebalanceListener.java
│       │   │
│       │   ├── schema/                           # Schema转换
│       │   │   ├── DynamicSchemaConverter.java
│       │   │   ├── JsonRecordParser.java
│       │   │   └── PathResolver.java             # 路径模板解析(含日期追加)
│       │   │
│       │   ├── buffer/                           # 写缓冲
│       │   │   ├── WriteBuffer.java
│       │   │   ├── DoubleWriteBuffer.java
│       │   │   ├── WriteBufferManager.java
│       │   │   └── BackpressureController.java
│       │   │
│       │   ├── writer/                           # 文件写入
│       │   │   ├── FileWriterFactory.java
│       │   │   ├── ParquetFileWriter.java
│       │   │   ├── CsvFileWriter.java
│       │   │   ├── FormatWriter.java
│       │   │   └── FlushExecutor.java
│       │   │
│       │   ├── storage/                          # 存储抽象
│       │   │   ├── StorageAdapter.java
│       │   │   └── HadoopStorageAdapter.java
│       │   │
│       │   ├── checkpoint/                       # 容错
│       │   │   ├── Checkpoint.java
│       │   │   ├── CheckpointManager.java
│       │   │   ├── CrashRecoveryManager.java
│       │   │   └── IdempotentWriteChecker.java
│       │   │
│       │   │
│       │   ├── node/
│       │   │   └── NodeIdentity.java
│       │   │
│       │   ├── metrics/
│       │   │   └── WriterMetrics.java            # Prometheus指标(仅/actuator)
│       │   │
│       │   └── lifecycle/
│       │       └── GracefulShutdownHook.java
│       │
│       └── resources/
│           ├── application.yml                   # Worker配置(Kafka/HDFS/MySQL只读)
│           └── application-prod.yml
│
│── =====================================================================
│   lake-writer-admin: 管理服务(Web界面 + REST API, 无Kafka消费)
│── =====================================================================
├── lake-writer-admin/
│   ├── pom.xml
│   ├── frontend/                                 # Vue.js前端源码
│   │   ├── package.json
│   │   ├── vue.config.js
│   │   ├── public/
│   │   │   └── index.html
│   │   └── src/
│   │       ├── main.js
│   │       ├── App.vue
│   │       ├── router/
│   │       │   └── index.js
│   │       ├── api/
│   │       │   ├── config.js                     # Topic配置CRUD
│   │       │   └── status.js                     # 消费状态查询
│   │       ├── views/
│   │       │   ├── Dashboard.vue                 # 仪表盘(汇总各Worker状态)
│   │       │   ├── ConfigList.vue                # 配置列表
│   │       │   ├── ConfigEdit.vue                # 配置编辑(Topic/Schema/路径)
│   │       │   ├── MonitorDetail.vue             # 消费详情(各Worker分区进度)
│   │       │   ├── ChangeLog.vue                 # 变更审计历史
│   │       │   └── NodeList.vue                  # Worker节点状态列表
│   │       └── components/
│   │           ├── SchemaEditor.vue              # Schema可视化编辑器
│   │           ├── PathPreview.vue               # 路径模板实时预览
│   │           └── ConsumerLagChart.vue          # 消费延迟图表
│   └── src/main/
│       ├── java/com/lakewriter/admin/
│       │   ├── AdminApplication.java             # Spring Boot启动类(Web服务)
│       │   ├── controller/
│       │   │   ├── ConfigController.java         # 配置CRUD REST API
│       │   │   ├── StatusController.java         # 消费状态查询(聚合Worker心跳)
│       │   │   └── ChangeLogController.java      # 变更历史查询
│       │   ├── service/
│       │   │   ├── ConfigService.java            # 配置管理业务逻辑
│       │   │   ├── StatusAggregator.java         # 聚合多Worker状态
│       │   │   └── ConfigValidator.java          # 配置校验(Schema/路径合法性)
│       │   └── config/
│       │       └── WebMvcConfig.java             # SPA路由, 静态资源配置
│       └── resources/
│           ├── application.yml                   # Admin配置(MySQL读写, 无Kafka/HDFS)
│           └── static/                           # Vue.js编译产物(npm run build)
│
│── =====================================================================
│   测试
│── =====================================================================
├── lake-writer-worker/src/test/
│   └── java/com/lakewriter/worker/
│       ├── consumer/
│       │   └── KafkaConsumerPoolTest.java
│       ├── schema/
│       │   ├── DynamicSchemaConverterTest.java
│       │   └── PathResolverTest.java
│       ├── buffer/
│       │   └── WriteBufferManagerTest.java
│       ├── writer/
│       │   └── ParquetFileWriterTest.java
│       ├── checkpoint/
│       │   └── CrashRecoveryManagerTest.java
│       └── config/
│           └── ConfigPollerTest.java
└── README.md
```

## 二、模块依赖关系

### 2.1 Maven模块依赖

```
kafka-lake-writer (parent pom)
├── lake-writer-common        # 公共模块: 实体类, DAO
├── lake-writer-worker        # 消费节点: 依赖common
│   └── depends on: common
└── lake-writer-admin         # 管理服务: 依赖common
    └── depends on: common

Worker和Admin之间无任何依赖, 完全独立部署
```

### 2.2 Worker内部模块依赖

```
          ┌──────────────────────────────┐
          │     ConfigPoller (只读MySQL)  │
          │     + TopicMatcher            │
          └──────────────┬───────────────┘
                         │ 通知变更
          ┌──────────────▼──────────────┐
          │      Consumer Pool           │◄── SafeRebalanceListener
          │  (KafkaConsumerPool)         │
          └──────────────┬──────────────┘
                         │ 分发消息
          ┌──────────────▼──────────────┐
          │     Record Dispatcher        │──── JsonRecordParser
          │                              │──── PathResolver
          └──────────────┬──────────────┘
                         │ 写入Buffer
          ┌──────────────▼──────────────┐
          │      Buffer Manager          │──── BackpressureController
          │  (WriteBufferManager)        │
          └──────────────┬──────────────┘
                         │ 触发flush
          ┌──────────────▼──────────────┐
          │      Flush Executor          │──── CheckpointManager
          │  (FileWriterFactory)         │──── IdempotentWriteChecker
          └──────────────┬──────────────┘
                         │ 写入文件
          ┌──────────────▼──────────────┐
          │     Storage Adapter          │
          │  (HadoopStorageAdapter)      │
          │  HDFS / OSS / S3             │
          └─────────────────────────────┘

          ┌──────────────────────────────┐
          │  HeartbeatReporter           │  并行: 每15秒上报状态到MySQL
          │  (独立定时线程, 不影响消费)   │
          └──────────────────────────────┘
```

### 2.3 Admin内部模块依赖

```
          ┌──────────────────────────────┐
          │  Vue.js Web界面              │
          │  (浏览器端, 调用REST API)     │
          └──────────────┬───────────────┘
                         │ HTTP
          ┌──────────────▼──────────────┐
          │  REST Controllers            │
          │  ConfigController            │
          │  StatusController            │
          │  ChangeLogController         │
          └──────────────┬───────────────┘
                         │
          ┌──────────────▼──────────────┐
          │  Services                    │
          │  ConfigService (配置CRUD)    │
          │  ConfigValidator (校验)      │
          │  StatusAggregator (聚合)     │
          └──────────────┬───────────────┘
                         │
          ┌──────────────▼──────────────┐
          │  MySQL (读写)                │
          │  topic_sink_config (配置)    │
          │  config_change_log (审计)    │
          │  worker_heartbeat (状态)     │
          └─────────────────────────────┘
```

## 三、核心接口定义

### 3.1 FormatWriter — 文件格式写入接口

```java
/**
 * 文件格式写入器接口
 * 不同格式(Parquet/CSV)实现此接口
 */
public interface FormatWriter extends AutoCloseable {

    /**
     * 初始化写入器
     * @param filePath 目标文件路径(临时文件)
     * @param schema   Schema定义
     * @param config   写入配置(压缩算法等)
     */
    void open(Path filePath, SchemaDefinition schema, TopicSinkConfig config) throws IOException;

    /**
     * 写入一行数据
     * @param row 字段值数组，顺序与Schema一致
     */
    void writeRow(Object[] row) throws IOException;

    /**
     * 写入多行数据(批量)
     */
    default void writeRows(List<Object[]> rows) throws IOException {
        for (Object[] row : rows) {
            writeRow(row);
        }
    }

    /**
     * 关闭写入器，完成文件
     */
    @Override
    void close() throws IOException;

    /**
     * 获取已写入的字节数(估算)
     */
    long getWrittenBytes();

    /**
     * 获取已写入的行数
     */
    long getWrittenRows();
}
```

### 3.2 StorageAdapter — 存储操作接口

```java
/**
 * 存储操作抽象接口
 * 屏蔽HDFS/OSS/S3差异
 */
public interface StorageAdapter extends AutoCloseable {

    /**
     * 创建输出流(用于写入临时文件)
     */
    OutputStream create(String path) throws IOException;

    /**
     * 原子重命名(临时文件 → 正式文件)
     */
    boolean rename(String srcPath, String dstPath) throws IOException;

    /**
     * 检查文件是否存在
     */
    boolean exists(String path) throws IOException;

    /**
     * 删除文件
     */
    boolean delete(String path) throws IOException;

    /**
     * 列出目录下的文件
     */
    List<String> listFiles(String dirPath) throws IOException;

    /**
     * 创建目录(递归)
     */
    boolean mkdirs(String path) throws IOException;
}
```

## 四、application.yml 配置结构

### 4.1 Worker 消费节点配置

```yaml
# lake-writer-worker/src/main/resources/application.yml
server:
  port: 8081                                         # Worker端口(仅暴露actuator)

spring:
  application:
    name: lake-writer-worker
  # 无Web模式: 不启动Servlet容器(除了actuator)
  main:
    web-application-type: none
  datasource:
    url: jdbc:mysql://localhost:3306/lake_writer?useSSL=false&serverTimezone=Asia/Shanghai
    username: ${DB_USERNAME:root}
    password: ${DB_PASSWORD:root}
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 3                           # Worker只需少量连接(轮询+心跳)
      minimum-idle: 1

# ===== Kafka消费配置 =====
lake-writer:
  kafka:
    bootstrap-servers: ${KAFKA_SERVERS:localhost:9092}
    group-id: ${GROUP_ID:kafka-lake-writer}
    consumer-count: ${CONSUMER_COUNT:4}
    max-poll-records: 5000
    fetch-min-bytes: 1048576
    fetch-max-wait-ms: 500
    session-timeout-ms: 30000
    heartbeat-interval-ms: 10000
    max-poll-interval-ms: 300000
    key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

  # ===== 存储配置 =====
  storage:
    type: hdfs
    hdfs:
      default-fs: ${HDFS_URL:hdfs://namenode:8020}
      config:
        dfs.replication: "3"
        dfs.client.block.write.replace-datanode-on-failure.enable: "true"
        dfs.client.use.datanode.hostname: "false"
    oss:
      endpoint: ${OSS_ENDPOINT:}
      access-key-id: ${OSS_ACCESS_KEY:}
      access-key-secret: ${OSS_SECRET_KEY:}
      bucket: ${OSS_BUCKET:}

  buffer:
    max-total-bytes: 3221225472                      # 3GB
    flush-thread-pool-size: 4

  config:
    sync-interval-sec: 30                            # 配置拉取间隔
    version-align-delay-sec: 60

  heartbeat:
    interval-sec: 15                                 # 心跳上报间隔

  checkpoint:
    dir: ${CHECKPOINT_DIR:./checkpoint}

  node:
    id: ${POD_NAME:${HOSTNAME:}}

# 仅暴露actuator(健康检查+Prometheus), 不暴露任何业务API
management:
  server:
    port: 8081
  endpoints:
    web:
      exposure:
        include: health,prometheus
  metrics:
    tags:
      application: lake-writer-worker
      node: ${POD_NAME:${HOSTNAME:unknown}}
```

### 4.2 Admin 管理服务配置

```yaml
# lake-writer-admin/src/main/resources/application.yml
server:
  port: 8080                                         # Admin Web服务端口

spring:
  application:
    name: lake-writer-admin
  datasource:
    url: jdbc:mysql://localhost:3306/lake_writer?useSSL=false&serverTimezone=Asia/Shanghai
    username: ${DB_USERNAME:root}
    password: ${DB_PASSWORD:root}
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 3
  # 前端静态资源
  resources:
    static-locations: classpath:/static/

# Admin不需要Kafka/HDFS/Buffer/Checkpoint配置
# 仅管理MySQL中的配置数据 + 读取Worker心跳状态

management:
  endpoints:
    web:
      exposure:
        include: health,info
```

## 五、Maven依赖 (pom.xml 关键部分)

```xml
<properties>
    <java.version>1.8</java.version>
    <spring-boot.version>2.7.18</spring-boot.version>
    <kafka.version>2.8.2</kafka.version>
    <parquet.version>1.12.3</parquet.version>
    <hadoop.version>3.0.0-cdh6.3.1</hadoop.version>
    <fastjson2.version>2.0.49</fastjson2.version>
</properties>

<repositories>
    <!-- CDH6 Maven仓库 -->
    <repository>
        <id>cloudera</id>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
    </repository>
</repositories>

<dependencies>
    <!-- Spring Boot 2.7 (JDK 8兼容的最高版本) -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>

    <!-- Kafka (2.8.x兼容Kafka 1.x/2.x Broker) -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>${kafka.version}</version>
    </dependency>

    <!-- Parquet (1.12.x兼容CDH6的hadoop 3.0.0) -->
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

    <!-- Hadoop CDH6 -->
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

    <!-- JSON解析 (fastjson2支持JDK 8) -->
    <dependency>
        <groupId>com.alibaba.fastjson2</groupId>
        <artifactId>fastjson2</artifactId>
        <version>${fastjson2.version}</version>
    </dependency>

    <!-- CSV -->
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-csv</artifactId>
        <version>1.10.0</version>
    </dependency>

    <!-- 数据库 -->
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.33</version>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>

    <!-- 监控 -->
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>

    <!-- 工具 -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- OSS (按需引入) -->
    <!--
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-aliyun</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
    -->

    <!-- 测试 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>

<!-- 编译配置: 确保JDK 8字节码 -->
<build>
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
</build>
```
