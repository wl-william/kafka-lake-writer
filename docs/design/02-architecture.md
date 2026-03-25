# Kafka-Lake-Writer 架构设计

## 一、系统架构总览

**核心原则：管理与消费分离**

系统拆分为两个独立部署的服务：
- **Admin 管理服务**（1个实例）：Web可视化界面 + REST API，负责配置管理和状态展示
- **Worker 消费节点**（N个实例）：纯消费写入进程，只从MySQL拉取配置，不含任何管理功能

```
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  ┌──────────────────────────────────┐     ┌─────────────────────────────┐ │
│  │    lake-writer-admin (1个实例)    │     │       MySQL                 │ │
│  │    管理服务                       │     │                             │ │
│  │                                  │     │  topic_sink_config 表       │ │
│  │  ┌──────────────────────────┐   │     │  config_change_log 表       │ │
│  │  │ Vue.js Web管理界面       │   │     │  worker_heartbeat 表        │ │
│  │  │ 配置管理/消费监控/变更审计 │   │     │                             │ │
│  │  └──────────────────────────┘   │ R/W │                             │ │
│  │  ┌──────────────────────────┐   ├────►│                             │ │
│  │  │ REST API                 │   │     │                             │ │
│  │  │ /api/v1/configs (CRUD)   │   │     │                             │ │
│  │  │ /api/v1/status  (查询)   │   │     │                             │ │
│  │  └──────────────────────────┘   │     │                             │ │
│  └──────────────────────────────────┘     │                             │ │
│                                           │                             │ │
│  ┌────────────────────────────────────┐   │                             │ │
│  │  lake-writer-worker (N个实例)       │   │                             │ │
│  │  消费节点 (纯消费, 无Web/无管理API)  │   │                             │ │
│  │                                    │   │                             │ │
│  │  ┌────────────────────────────┐   │ R │                             │ │
│  │  │ ConfigPoller (只读轮询)    ├───┼──►│                             │ │
│  │  │ 每30秒拉取配置             │   │   │                             │ │
│  │  └─────────────┬──────────────┘   │   │                             │ │
│  │                │                  │   │                             │ │
│  │  ┌─────────────▼──────────────┐   │   │                             │ │
│  │  │ HeartbeatReporter          ├───┼──►│  上报心跳+消费状态           │ │
│  │  │ 每15秒上报状态             │   │ W │                             │ │
│  │  └────────────────────────────┘   │   │                             │ │
│  │                                    │   └─────────────────────────────┘ │
│  │  ┌────────────────────────────┐   │                                    │
│  │  │ Kafka Consumer Pool        │   │                                    │
│  │  │ → Write Buffer             │   │                                    │
│  │  │ → File Writer              │   │   ┌─────────────────────────────┐ │
│  │  │ → Checkpoint Manager       │   │   │       HDFS / OSS            │ │
│  │  │                            ├───┼──►│                             │ │
│  │  └────────────────────────────┘   │   └─────────────────────────────┘ │
│  └────────────────────────────────────┘                                    │
│                                                                            │
│  ┌────────────────────────────────────┐                                    │
│  │  lake-writer-worker (第2个实例)     │   同上，同一个Consumer Group      │
│  │  ...                               │                                    │
│  └────────────────────────────────────┘                                    │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 管理与消费分离的关键设计

| 职责         | Admin 管理服务 | Worker 消费节点                       |
| ---------- | ---------- | --------------------------------- |
| 配置CRUD     | ✅ 读写MySQL  | ❌ 只读MySQL                         |
| Web界面      | ✅ Vue.js前端 | ❌ 无                               |
| REST API   | ✅ 完整API    | ❌ 仅暴露 /actuator (健康检查+Prometheus) |
| Kafka消费    | ❌ 不消费      | ✅ 核心职责                            |
| HDFS/OSS写入 | ❌ 不写入      | ✅ 核心职责                            |
| 状态上报       | ❌ 从MySQL读  | ✅ 心跳+状态写入MySQL                    |
| 部署实例数      | 1个         | N个(按吞吐需求扩缩)                       |
| 资源要求       | 1C2G足够     | 4C8G                              |

## 二、Worker消费节点分层职责

### 2.1 配置拉取层 (ConfigPoller)

**职责：** 只读轮询MySQL，获取最新配置，驱动消费行为变更。

- 每30秒 `SELECT * FROM topic_sink_config WHERE status='ACTIVE'`
- 计算配置diff：新增topic、移除topic、schema变更、路径变更
- 配置变更时通知消费层和缓冲层做相应调整
- 支持正则匹配topic（如 `order_.*` 匹配所有order开头的topic）
- **不写入MySQL配置表，不提供任何REST API**

### 2.2 心跳上报层 (HeartbeatReporter)

**职责：** 定期向MySQL写入本节点状态，供Admin服务聚合展示。

- 每15秒上报：节点ID、分配到的分区列表、各分区消费offset/lag、Buffer状态
- Admin服务读取此表展示所有Worker节点的消费情况

### 2.3 消费层 (Kafka Consumer Pool)

**职责：** 管理Kafka Consumer生命周期，负责数据拉取和分区分配。

- 维护一个Consumer Group，所有Worker节点共享同一个`group.id`
- Consumer数量可配置（建议等于CPU核数）
- 使用`subscribe()`模式订阅topic列表，支持动态变更
- 手动管理offset提交（`enable.auto.commit=false`）
- 实现`ConsumerRebalanceListener`处理Rebalance事件

### 2.3 缓冲层 (Write Buffer Layer)

**职责：** 内存中按topic+partition+路径分桶积累数据，达到阈值后触发flush。

- 每个`(topic, partition, pathKey)`组合对应一个独立的WriteBuffer
- 三重flush触发条件：行数阈值 / 字节数阈值 / 时间阈值
- 双Buffer交替策略：当Buffer-A在flush时，新数据写入Buffer-B，避免阻塞消费
- 背压控制：当Buffer总内存超过上限时，暂停Consumer poll

### 2.4 写入层 (File Writer Pool)

**职责：** 将Buffer数据序列化为目标格式，写入存储系统。

- 独立线程池执行写入操作，不阻塞消费线程
- 支持Parquet和CSV两种输出格式，通过工厂模式扩展
- 使用临时文件 + rename的两阶段写入，保证原子性
- 写入失败自动重试（最多3次，指数退避）

### 2.5 容错层 (Checkpoint Manager)

**职责：** 管理checkpoint文件，支持crash后自动恢复。

- 每次flush前在本地磁盘写入checkpoint
- flush完成 + offset提交后删除checkpoint
- 进程启动时扫描checkpoint目录，执行恢复逻辑
- 清理HDFS/OSS上的残留临时文件

### 2.6 存储抽象层 (Storage Adapter)

**职责：** 屏蔽HDFS/OSS/S3的差异，提供统一的文件操作接口。

- 基于Hadoop FileSystem API，原生支持HDFS
- OSS通过`hadoop-aliyun`插件或JindoFS SDK接入
- S3通过`hadoop-aws`插件接入
- 提供统一的`create/rename/delete/exists/list`操作

## 三、技术选型

> **环境约束：JDK 1.8、Kafka 1.x/2.x、HDFS 3.0.0-cdh6.3.1 (CDH6)**

| 层 | 选型 | 版本 | 理由 |
|----|------|------|------|
| JDK | Oracle/OpenJDK | 1.8 | 生产环境约束，代码需Java 8兼容 |
| 框架 | Spring Boot | 2.7.x | **JDK 8最高支持版本**，Spring Boot 3.x需JDK 17 |
| Kafka客户端 | kafka-clients | 2.8.2 | 兼容Kafka 1.x/2.x Broker，向下兼容 |
| JSON解析 | fastjson2 | 2.0.x | 支持JDK 8，高性能JSON解析 |
| Parquet写入 | parquet-hadoop | 1.12.3 | 兼容CDH6的hadoop 3.0.0，1.13+需hadoop 3.2+ |
| CSV写入 | Apache Commons CSV | 1.10.0 | 轻量稳定，JDK 8兼容 |
| 存储抽象 | Hadoop FileSystem API | 3.0.0-cdh6.3.1 | **使用CDH版hadoop-client**，统一HDFS/OSS/S3 |
| OSS接入 | hadoop-aliyun / JindoFS | - | 阿里云OSS Hadoop兼容 |
| 配置中心 | MySQL + Web管理界面 | 5.7+/8.0+ | MySQL存储 + Vue.js前端可视化管理 |
| 前端 | Vue.js 2.x + Element UI | - | 配置管理可视化界面，轻量SPA |
| 连接池 | HikariCP | 4.0.3 | Spring Boot 2.7默认连接池 |
| 监控 | Micrometer + Prometheus | - | Spring Boot 2.7原生支持 |
| 日志 | SLF4J + Logback | - | Spring Boot默认 |

### 3.1 CDH版本兼容说明

```xml
<!-- CDH6 Maven仓库 -->
<repository>
    <id>cloudera</id>
    <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
</repository>

<!-- 使用CDH版hadoop-client, 确保与HDFS集群版本一致 -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>3.0.0-cdh6.3.1</version>
</dependency>
```

### 3.2 JDK 8 代码约束

- 不使用Java 11+特性：`var`关键字、`switch`表达式、`text block`、`record`类型
- 不使用`Stream.toList()`（Java 16+），使用`Collectors.toList()`
- Lambda和Stream API可正常使用（Java 8原生支持）
- 不使用`HttpClient`（Java 11+），使用Apache HttpClient或OkHttp

## 四、数据流

```
     Kafka Broker
         │
         │  KafkaConsumer.poll() (batch, max.poll.records=5000)
         ▼
  ┌──────────────┐
  │ ConsumerRecord│  包含: topic, partition, offset, key, value(JSON字符串)
  │    batch      │
  └──────┬───────┘
         │
         │  RecordDispatcher: 根据topic查找TopicSinkConfig
         ▼
  ┌──────────────┐
  │ TopicSinkConfig│  包含: schema定义, 输出格式, 路径模板, flush策略
  └──────┬───────┘
         │
         │  DynamicSchemaConverter: JSON字符串 → 结构化记录
         │  PathResolver: 路径模板 + 消息字段 → 实际路径
         ▼
  ┌──────────────┐
  │ WriteBuffer  │  按(topic, partition, pathKey)分桶
  │ (内存)       │  记录: 数据行 + startOffset + endOffset
  └──────┬───────┘
         │
         │  达到flush条件 (行数/大小/时间)
         ▼
  ┌──────────────┐
  │ Checkpoint   │  写入本地: {topic, partition, offsetRange, tmpFilePath}
  │ (本地磁盘)   │
  └──────┬───────┘
         │
         │  FileWriter: 序列化为Parquet/CSV
         ▼
  ┌──────────────┐
  │ 临时文件      │  路径: {sinkPath}/_tmp/part-{node}-{partition}-{offset}.tmp
  │ (HDFS/OSS)   │
  └──────┬───────┘
         │
         │  rename (原子操作)
         ▼
  ┌──────────────┐
  │ 正式文件      │  路径: {sinkPath}/part-{node}-P{n}-{startOffset}to{endOffset}-{ts}.snappy.parquet
  │ (HDFS/OSS)   │
  └──────┬───────┘
         │
         │  consumer.commitSync(offsets)
         │  删除checkpoint文件
         ▼
     完成一次flush周期
```
