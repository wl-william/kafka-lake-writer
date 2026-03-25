# Kafka-Lake-Writer 数据模型设计

## 一、配置数据模型

### 1.1 Topic Sink 配置表

```sql
CREATE TABLE `topic_sink_config` (
    `id`                 BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Topic匹配
    `topic_name`         VARCHAR(255) NOT NULL COMMENT 'Kafka topic名称，支持正则',
    `match_type`         VARCHAR(16)  NOT NULL DEFAULT 'EXACT' COMMENT '匹配方式: EXACT精确匹配, REGEX正则匹配',

    -- Schema定义
    `schema_json`        TEXT         NOT NULL COMMENT 'JSON Schema定义，字段名+类型数组',

    -- 写入配置
    `sink_format`        VARCHAR(16)  NOT NULL DEFAULT 'PARQUET' COMMENT '写入格式: PARQUET, CSV',
    `sink_path`          VARCHAR(512) NOT NULL COMMENT '目标路径模板，支持变量: {topic}, {dt}, {hour}',
    `partition_by`       VARCHAR(255)          DEFAULT NULL COMMENT '分区字段，逗号分隔，如: dt,hour',
    `compression`        VARCHAR(32)  NOT NULL DEFAULT 'SNAPPY' COMMENT '压缩算法: SNAPPY, GZIP, ZSTD, NONE',

    -- Flush策略
    `flush_rows`         INT          NOT NULL DEFAULT 500000 COMMENT 'flush行数阈值',
    `flush_bytes`        BIGINT       NOT NULL DEFAULT 268435456 COMMENT 'flush字节数阈值(默认256MB)',
    `flush_interval_sec` INT          NOT NULL DEFAULT 600 COMMENT 'flush时间间隔(默认10分钟)',

    -- 状态管理
    `status`             VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE' COMMENT '状态: ACTIVE启用, PAUSED暂停',
    `version`            INT          NOT NULL DEFAULT 1 COMMENT '乐观锁版本号，每次更新+1',
    `description`        VARCHAR(512)          DEFAULT NULL COMMENT '配置描述',

    -- 时间戳
    `created_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY `uk_topic_name` (`topic_name`),
    KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Topic写入配置表';
```

### 1.2 Schema JSON 格式定义

```json
{
  "fields": [
    {"name": "user_id",    "type": "LONG",    "nullable": true},
    {"name": "event_type", "type": "STRING",  "nullable": false},
    {"name": "amount",     "type": "DOUBLE",  "nullable": true},
    {"name": "timestamp",  "type": "LONG",    "nullable": false},
    {"name": "dt",         "type": "STRING",  "nullable": false},
    {"name": "extra_info", "type": "STRING",  "nullable": true}
  ]
}
```

**支持的字段类型：**

| 类型 | Java类型 | Parquet类型 | CSV处理 |
|------|---------|------------|---------|
| STRING | String | BINARY (UTF8) | 原样输出 |
| INT | Integer | INT32 | 原样输出 |
| LONG | Long | INT64 | 原样输出 |
| FLOAT | Float | FLOAT | 原样输出 |
| DOUBLE | Double | DOUBLE | 原样输出 |
| BOOLEAN | Boolean | BOOLEAN | true/false |

### 1.3 路径模板变量

| 变量 | 来源 | 示例 | 说明 |
|------|------|------|------|
| `{topic}` | Kafka消息的topic名 | orders | 固定值 |
| `{date}` | 系统当前日期 yyyy-MM-dd | 2026-03-22 | **自动按天生成目录** |
| `{dt}` | 消息字段`dt`，缺失则用`{date}` | 2026-03-22 | 优先取消息字段 |
| `{hour}` | 消息字段`hour`，缺失则用当前小时 | 14 | 可选小时分区 |
| `{minute}` | 消息字段`minute`，缺失则用当前分钟 | 30 | 可选分钟分区 |
| `{partition}` | Kafka partition编号 | 3 | Kafka分区号 |

**核心场景：每天增量接入，路径自动追加日期**

每个Topic可配置独立的HDFS/OSS路径前缀，路径末尾使用`{date}`变量自动按天生成子目录：

**示例1：日期作为子目录（推荐，与用户需求一致）**
```
配置: /hdfs/kafka/data/dev_stats/{date}
解析: /hdfs/kafka/data/dev_stats/2026-03-22
次日: /hdfs/kafka/data/dev_stats/2026-03-23   ← 自动切换，无需人工干预
```

**示例2：Hive分区风格**
```
配置: /data/warehouse/{topic}/dt={dt}/hour={hour}/
解析: /data/warehouse/orders/dt=2026-03-22/hour=14/
```

**示例3：不同Topic写入完全不同的路径**
```
Topic: orders     → /hdfs/kafka/data/order_center/{date}
Topic: user_logs  → /hdfs/kafka/data/user_behavior/{date}
Topic: sys_monitor→ /oss://bucket/logs/monitor/{date}
```

**日期切换逻辑：**
- 以消息到达时的系统日期为准（非消息内时间戳）
- 跨天时（23:59→00:00），新数据自动写入新日期目录
- 旧日期的Buffer会在flush时写入旧日期目录（不会混入新日期）

## 二、运行时数据模型

### 2.1 Checkpoint 文件格式

**存储位置：** 本地磁盘 `./checkpoint/{topic}/{partition}.ckpt`

```json
{
  "topic": "orders",
  "partition": 3,
  "startOffset": 580000,
  "endOffset": 582390,
  "recordCount": 2390,
  "tmpFilePath": "/data/warehouse/orders/dt=2026-03-22/hour=14/_tmp/part-pod01-P3-580000to582390.tmp",
  "targetFilePath": "/data/warehouse/orders/dt=2026-03-22/hour=14/part-pod01-P3-580000to582390-20260322140530.snappy.parquet",
  "createdAt": "2026-03-22T14:05:30",
  "nodeId": "pod01"
}
```

### 2.2 输出文件命名规则

**命名格式：**
```
part-{nodeId}-P{partition}-{startOffset}to{endOffset}-{timestamp}.{compression}.{format}
```

**示例：**
```
part-pod01-P3-580000to582390-20260322140530.snappy.parquet
part-pod02-P5-120000to125000-20260322141022.gzip.csv
```

**命名各段含义：**

| 段 | 用途 |
|----|------|
| nodeId | 区分不同节点写入的文件，避免冲突 |
| partition | 标识数据来源分区，便于追踪 |
| startOffset~endOffset | 数据的offset范围，支持幂等检测 |
| timestamp | 文件创建时间，便于排查 |
| compression | 压缩算法标识 |
| format | 文件格式标识 |

### 2.3 临时文件规则

```
{sinkPath}/_tmp/part-{nodeId}-P{partition}-{startOffset}to{endOffset}.tmp
```

- 临时文件写入`_tmp/`子目录下
- flush完成后通过`rename`操作移动到正式目录
- 进程启动时清理所有`_tmp/`目录下的残留文件
- Hive/Spark读取时可通过路径过滤自动忽略`_tmp/`

## 三、配置示例

### 3.1 多Topic配置示例

```sql
-- 订单数据: 写入Parquet
INSERT INTO topic_sink_config
(topic_name, match_type, schema_json, sink_format, sink_path, partition_by, flush_rows, flush_interval_sec)
VALUES
('orders', 'EXACT',
 '{"fields":[{"name":"order_id","type":"LONG"},{"name":"user_id","type":"LONG"},{"name":"amount","type":"DOUBLE"},{"name":"status","type":"STRING"},{"name":"dt","type":"STRING"},{"name":"hour","type":"STRING"}]}',
 'PARQUET', '/data/warehouse/orders/dt={dt}/hour={hour}/', 'dt,hour', 500000, 600);

-- 用户行为日志: 写入Parquet，正则匹配多个topic
INSERT INTO topic_sink_config
(topic_name, match_type, schema_json, sink_format, sink_path, partition_by, flush_rows, flush_interval_sec)
VALUES
('user_event_.*', 'REGEX',
 '{"fields":[{"name":"uid","type":"LONG"},{"name":"event","type":"STRING"},{"name":"page","type":"STRING"},{"name":"ts","type":"LONG"},{"name":"dt","type":"STRING"}]}',
 'PARQUET', '/data/warehouse/user_events/{topic}/dt={dt}/', 'dt', 1000000, 300);

-- 系统监控日志: 写入CSV
INSERT INTO topic_sink_config
(topic_name, match_type, schema_json, sink_format, sink_path, compression, flush_rows, flush_interval_sec)
VALUES
('sys_monitor', 'EXACT',
 '{"fields":[{"name":"host","type":"STRING"},{"name":"cpu","type":"DOUBLE"},{"name":"mem","type":"DOUBLE"},{"name":"disk","type":"DOUBLE"},{"name":"ts","type":"LONG"},{"name":"dt","type":"STRING"}]}',
 'CSV', '/data/logs/monitor/dt={dt}/', 'GZIP', 200000, 120);
```
