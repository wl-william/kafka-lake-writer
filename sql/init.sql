-- ============================================================
-- Kafka-Lake-Writer 数据库初始化脚本
-- ============================================================

CREATE DATABASE IF NOT EXISTS `lake_writer` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `lake_writer`;

-- ============================================================
-- 1. Topic Sink 配置表
-- ============================================================
CREATE TABLE IF NOT EXISTS `topic_sink_config` (
    `id`                 BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Topic匹配
    `topic_name`         VARCHAR(255) NOT NULL COMMENT 'Kafka topic名称，REGEX模式下为正则表达式',
    `match_type`         VARCHAR(16)  NOT NULL DEFAULT 'EXACT' COMMENT '匹配方式: EXACT精确匹配, REGEX正则匹配',

    -- Schema定义
    `schema_json`        TEXT         NOT NULL COMMENT 'JSON Schema定义, 格式: {"fields":[{"name":"xxx","type":"STRING","nullable":true}]}',

    -- 写入配置
    `sink_format`        VARCHAR(16)  NOT NULL DEFAULT 'PARQUET' COMMENT '写入格式: PARQUET, CSV',
    `sink_path`          VARCHAR(512) NOT NULL COMMENT '目标路径模板, 支持变量: {topic}, {dt}, {hour}, {minute}, {partition}',
    `partition_by`       VARCHAR(255)          DEFAULT NULL COMMENT '路径分区字段, 逗号分隔, 如: dt,hour',
    `compression`        VARCHAR(32)  NOT NULL DEFAULT 'SNAPPY' COMMENT '压缩算法: SNAPPY, GZIP, ZSTD, NONE',

    -- Flush策略
    `flush_rows`         INT          NOT NULL DEFAULT 500000 COMMENT 'flush行数阈值, 达到此行数触发写入',
    `flush_bytes`        BIGINT       NOT NULL DEFAULT 268435456 COMMENT 'flush字节数阈值(默认256MB)',
    `flush_interval_sec` INT          NOT NULL DEFAULT 600 COMMENT 'flush时间间隔秒数(默认10分钟)',

    -- 状态管理
    `status`             VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE' COMMENT '状态: ACTIVE启用, PAUSED暂停',
    `version`            INT          NOT NULL DEFAULT 1 COMMENT '乐观锁版本号, 每次更新自动+1',
    `description`        VARCHAR(512)          DEFAULT NULL COMMENT '配置描述',

    -- 时间戳
    `created_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY `uk_topic_name` (`topic_name`),
    KEY `idx_status` (`status`),
    KEY `idx_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Topic写入配置表';

-- ============================================================
-- 2. 配置变更审计表 (可选, 记录谁在什么时候改了什么)
-- ============================================================
CREATE TABLE IF NOT EXISTS `config_change_log` (
    `id`             BIGINT AUTO_INCREMENT PRIMARY KEY,
    `config_id`      BIGINT       NOT NULL COMMENT '关联的topic_sink_config.id',
    `topic_name`     VARCHAR(255) NOT NULL COMMENT 'Topic名称(冗余存储方便查询)',
    `change_type`    VARCHAR(16)  NOT NULL COMMENT 'CREATED, UPDATED, DELETED, PAUSED, RESUMED',
    `old_value`      TEXT                  DEFAULT NULL COMMENT '变更前配置(JSON)',
    `new_value`      TEXT                  DEFAULT NULL COMMENT '变更后配置(JSON)',
    `operator`       VARCHAR(64)           DEFAULT NULL COMMENT '操作人',
    `created_at`     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    KEY `idx_config_id` (`config_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='配置变更审计日志';

-- ============================================================
-- 3. Worker心跳状态表 (Worker上报, Admin读取展示)
-- ============================================================
CREATE TABLE IF NOT EXISTS `worker_heartbeat` (
    -- node_id作为主键, 每个Worker节点有唯一一行, upsert时ON DUPLICATE KEY UPDATE
    `node_id`            VARCHAR(128) NOT NULL PRIMARY KEY COMMENT 'Worker节点ID(hostname/pod名)',
    `status`             VARCHAR(16)  NOT NULL DEFAULT 'ONLINE' COMMENT 'ONLINE, OFFLINE',

    -- 分区分配信息
    `assigned_partitions`TEXT                  DEFAULT NULL COMMENT '当前分配的分区列表(JSON), 如: {"orders":[0,1,2],"logs":[0,1]}',

    -- 消费状态
    `consumer_status`    TEXT                  DEFAULT NULL COMMENT '各分区消费状态(JSON), 含offset/lag/bufferRows等',

    -- 系统指标
    `records_per_sec`    DOUBLE       NOT NULL DEFAULT 0 COMMENT '当前消费速率(records/s)',
    `buffer_usage_bytes` BIGINT       NOT NULL DEFAULT 0 COMMENT 'Buffer总使用字节数',
    `uptime_sec`         BIGINT       NOT NULL DEFAULT 0 COMMENT '运行时长(秒)',

    -- 心跳时间
    `last_heartbeat`     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY `idx_last_heartbeat` (`last_heartbeat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Worker节点心跳状态表(Worker写, Admin读)';

-- ============================================================
-- 4. 示例数据
-- ============================================================

-- 订单数据: 按天增量写入，路径自动追加日期
INSERT INTO `topic_sink_config`
(`topic_name`, `match_type`, `schema_json`, `sink_format`, `sink_path`, `partition_by`,
 `compression`, `flush_rows`, `flush_bytes`, `flush_interval_sec`, `description`)
VALUES
('orders', 'EXACT',
 '{"fields":[{"name":"order_id","type":"LONG","nullable":false},{"name":"user_id","type":"LONG","nullable":false},{"name":"amount","type":"DOUBLE","nullable":true},{"name":"status","type":"STRING","nullable":true},{"name":"created_time","type":"STRING","nullable":true}]}',
 'PARQUET',
 '/hdfs/kafka/data/orders/{date}',
 NULL,
 'SNAPPY', 500000, 268435456, 600,
 '订单数据, 按天增量写入, 路径自动追加日期如/hdfs/kafka/data/orders/2026-03-22');

-- 设备统计: 每天增量接入
INSERT INTO `topic_sink_config`
(`topic_name`, `match_type`, `schema_json`, `sink_format`, `sink_path`, `partition_by`,
 `compression`, `flush_rows`, `flush_bytes`, `flush_interval_sec`, `description`)
VALUES
('dev_stats', 'EXACT',
 '{"fields":[{"name":"device_id","type":"LONG","nullable":false},{"name":"metric_name","type":"STRING","nullable":false},{"name":"metric_value","type":"DOUBLE","nullable":true},{"name":"ts","type":"LONG","nullable":false}]}',
 'PARQUET',
 '/hdfs/kafka/data/dev_stats/{date}',
 NULL,
 'SNAPPY', 500000, 268435456, 600,
 '设备统计数据, 按天增量写入, 如/hdfs/kafka/data/dev_stats/2026-03-22');

-- 用户行为日志: 正则匹配多个topic, 按topic+日期路由
INSERT INTO `topic_sink_config`
(`topic_name`, `match_type`, `schema_json`, `sink_format`, `sink_path`, `partition_by`,
 `compression`, `flush_rows`, `flush_bytes`, `flush_interval_sec`, `description`)
VALUES
('user_event_.*', 'REGEX',
 '{"fields":[{"name":"uid","type":"LONG","nullable":false},{"name":"event","type":"STRING","nullable":false},{"name":"page","type":"STRING","nullable":true},{"name":"duration_ms","type":"LONG","nullable":true},{"name":"ts","type":"LONG","nullable":false}]}',
 'PARQUET',
 '/hdfs/kafka/data/user_events/{topic}/{date}',
 NULL,
 'SNAPPY', 1000000, 536870912, 300,
 '用户行为日志, 正则匹配user_event_开头的所有topic, 按topic+日期路由');

-- 系统监控: CSV格式, 按天增量
INSERT INTO `topic_sink_config`
(`topic_name`, `match_type`, `schema_json`, `sink_format`, `sink_path`, `partition_by`,
 `compression`, `flush_rows`, `flush_bytes`, `flush_interval_sec`, `description`)
VALUES
('sys_monitor', 'EXACT',
 '{"fields":[{"name":"host","type":"STRING","nullable":false},{"name":"cpu_usage","type":"DOUBLE","nullable":true},{"name":"mem_usage","type":"DOUBLE","nullable":true},{"name":"disk_usage","type":"DOUBLE","nullable":true},{"name":"ts","type":"LONG","nullable":false}]}',
 'CSV',
 '/hdfs/kafka/data/monitor/{date}',
 NULL,
 'GZIP', 200000, 134217728, 120,
 '系统监控指标, CSV+Gzip压缩, 2分钟flush, 按天增量写入');
