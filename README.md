# Kafka-Lake-Writer

轻量级、高性能的 Kafka 数据同步组件，消费 Kafka JSON 数据写入 HDFS/OSS，支持 Parquet、CSV 格式。

## 核心特性

- **高性能**：单节点 4C8G 支持 6W+ records/s 吞吐
- **动态 Topic**：运行时新增/移除/暂停 Topic，无需重启
- **动态 Schema**：运行时变更字段定义，自动切换
- **动态路径**：每个Topic自定义HDFS/OSS路径，支持自动按天追加日期目录（如 `/hdfs/kafka/data/dev_stats/2026-03-22`）
- **Web管理界面**：可视化配置管理、Schema编辑、消费监控、变更审计
- **多格式**：Parquet（Snappy/Gzip/Zstd）、CSV
- **多存储**：HDFS（CDH6）、阿里云 OSS（S3 兼容）
- **容错**：At-Least-Once 语义，crash 自动恢复，不丢数据
- **多节点**：水平扩展，Kafka Consumer Group 自动负载均衡
- **小文件控制**：行数/字节数/时间三重阈值，避免产生大量小文件
- **监控**：Prometheus 指标，消费延迟、写入吞吐、Buffer 使用率

## 架构概览

**管理与消费分离：** 两个独立服务，通过MySQL间接通信。

```
┌──────────────────────┐       ┌────────┐       ┌─────────────────────────┐
│ lake-writer-admin    │       │ MySQL  │       │ lake-writer-worker (N个) │
│ (1个实例, Web管理)    │ R/W   │        │ R     │ (纯消费, 无Web)          │
│ Vue.js + REST API   ├──────►│ config ├──────►│ ConfigPoller(只读)       │
│ 配置管理/消费监控     │◄──────┤ heart  │◄──────┤ HeartbeatReporter(写)   │
└──────────────────────┘       └────────┘       │ Consumer → Buffer → HDFS│
                                                └─────────────────────────┘
```

- **Admin 管理服务**（1个实例）：Web界面 + REST API，配置管理、状态展示、变更审计
- **Worker 消费节点**（N个实例）：纯消费写入，只读MySQL拉配置，心跳上报状态
- Admin宕机不影响Worker消费，Worker只依赖MySQL和Kafka

## 设计文档

| 文档 | 描述 |
|------|------|
| [01-需求文档](docs/design/01-requirements.md) | 功能需求、性能需求、环境约束（JDK 8/Kafka 1.x/CDH6） |
| [02-架构设计](docs/design/02-architecture.md) | 系统架构、分层职责、技术选型、CDH兼容、数据流 |
| [03-数据模型](docs/design/03-data-model.md) | 配置表、Schema格式、文件命名、日期路径模板 |
| [04-容错机制](docs/design/04-fault-tolerance.md) | Offset管理、五阶段提交、Crash恢复、降级模式 |
| [05-多节点部署](docs/design/05-multi-node.md) | Consumer Group、Rebalance处理、扩缩容、K8s部署 |
| [06-动态配置](docs/design/06-dynamic-config.md) | 配置热更新、Schema变更、REST API、**Web可视化管理界面** |
| [07-性能设计](docs/design/07-performance.md) | 各层优化、内存管理、JDK 8 JVM调优、性能基准 |
| [08-项目结构](docs/design/08-project-structure.md) | 模块划分（含前端）、接口定义、Maven依赖（CDH6）、配置文件 |

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 1.8 | 生产环境约束 |
| Spring Boot | 2.7.x | JDK 8兼容的最高版本 |
| Kafka Broker | 1.x / 2.x | 生产Kafka版本 |
| kafka-clients | 2.8.2 | 向下兼容Kafka 1.x/2.x |
| HDFS | 3.0.0-cdh6.3.1 | CDH6发行版 |
| Parquet | 1.12.3 | 兼容CDH6 hadoop |
| MySQL | 5.7+ / 8.0+ | 配置中心存储 |
| 前端 | Vue.js 2.x + Element UI | Web管理界面 |

## 快速开始

### 1. 初始化数据库

```bash
mysql -u root -p < sql/init.sql
```

### 2. 启动Admin管理服务

```bash
java -jar lake-writer-admin.jar
# 访问 http://localhost:8080/ 进入Web管理界面
```

### 3. 启动Worker消费节点

```bash
java -Xms5g -Xmx5g -XX:+UseG1GC -jar lake-writer-worker.jar
# 可启动多个实例, 同一Consumer Group自动负载均衡
```

### 4. 在Web界面添加Topic配置，或通过API

```bash
curl -X POST http://localhost:8080/api/v1/configs \
  -H 'Content-Type: application/json' \
  -d '{
    "topicName": "dev_stats",
    "matchType": "EXACT",
    "schemaJson": {"fields":[{"name":"device_id","type":"LONG"},{"name":"metric","type":"STRING"},{"name":"value","type":"DOUBLE"},{"name":"ts","type":"LONG"}]},
    "sinkFormat": "PARQUET",
    "sinkPath": "/hdfs/kafka/data/dev_stats/{date}",
    "compression": "SNAPPY",
    "flushRows": 500000,
    "flushIntervalSec": 600
  }'
```

配置将在 30 秒内自动生效，无需重启。写入路径自动按天生成：`/hdfs/kafka/data/dev_stats/2026-03-22`。

## 运维操作

| 操作 | 方式 |
|------|------|
| 新增 Topic | Web界面 或 POST /api/v1/configs，30秒自动生效 |
| 暂停 Topic | Web界面一键暂停 或 PUT /api/v1/configs/{id}/pause |
| 变更 Schema | Web界面可视化编辑字段，自动flush旧数据后切换 |
| 变更路径 | Web界面修改路径模板，实时预览解析结果 |
| 查看消费状态 | Web界面仪表盘 或 GET /api/v1/status |
| 变更审计 | Web界面变更历史页面，查看每次配置修改记录 |
| 监控指标 | GET /actuator/prometheus |
| 扩容 | 直接启动新节点（相同group.id），自动Rebalance |
