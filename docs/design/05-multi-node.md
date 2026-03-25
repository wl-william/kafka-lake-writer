# Kafka-Lake-Writer 多节点部署设计

## 一、核心思路

利用 **Kafka Consumer Group** 天然的分区分配机制实现多节点负载均衡：

- 所有节点使用**同一个 `group.id`**
- Kafka保证：**一个Partition在同一时刻只会被Consumer Group中的一个Consumer消费**
- 节点增减时，Kafka自动触发Rebalance，重新分配Partition

## 二、多节点架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                    配置中心 (MySQL / Nacos)                          │
│              所有节点共享同一份topic_sink_config                      │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │ poll (30s)                           │ poll (30s)
┌──────────▼──────────┐            ┌──────────────▼──────────┐
│      Node-1          │            │      Node-2              │
│      (4C8G)          │            │      (4C8G)              │
│                      │            │                          │
│  group.id =          │            │  group.id =              │
│  "kafka-lake-writer" │            │  "kafka-lake-writer"     │
│                      │            │                          │
│  分配到:             │            │  分配到:                 │
│  orders/P0, P1, P2   │            │  orders/P3, P4, P5       │
│  logs/P0, P1         │            │  logs/P2, P3             │
│                      │            │                          │
│  Buffer:             │            │  Buffer:                 │
│   orders/P0 → buf-0  │            │   orders/P3 → buf-3      │
│   orders/P1 → buf-1  │            │   orders/P4 → buf-4      │
│   orders/P2 → buf-2  │            │   orders/P5 → buf-5      │
│   logs/P0   → buf-a  │            │   logs/P2   → buf-c      │
│   logs/P1   → buf-b  │            │   logs/P3   → buf-d      │
│                      │            │                          │
│  Checkpoint:         │            │  Checkpoint:             │
│   本地: ./ckpt/      │            │   本地: ./ckpt/          │
│                      │            │                          │
│  写入文件名前缀:     │            │  写入文件名前缀:         │
│   part-node1-...     │            │   part-node2-...         │
└──────────┬───────────┘            └──────────────┬───────────┘
           │                                       │
           ▼                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                         HDFS / OSS                               │
│                                                                  │
│  /data/warehouse/orders/dt=2026-03-22/hour=14/                  │
│    part-node1-P0-0to500000-20260322140000.snappy.parquet        │
│    part-node1-P1-0to480000-20260322140100.snappy.parquet        │
│    part-node2-P3-0to520000-20260322140030.snappy.parquet        │
│    part-node2-P4-0to490000-20260322140200.snappy.parquet        │
│                                                                  │
│  不同节点写入不同文件名，同一目录下多个文件，互不冲突              │
└──────────────────────────────────────────────────────────────────┘
```

## 三、节点标识

每个节点需要一个**全局唯一的标识**，嵌入到输出文件名中，避免文件冲突。

```java
public class NodeIdentity {

    private final String nodeId;

    public NodeIdentity() {
        // 优先级: POD_NAME > HOSTNAME > 环境变量 > 随机生成
        this.nodeId = Stream.<Supplier<String>>of(
                () -> System.getenv("POD_NAME"),          // K8s Pod名
                () -> System.getenv("HOSTNAME"),          // Docker容器名
                () -> System.getenv("NODE_ID"),           // 手动指定
                () -> getHostName(),                      // 主机名
                () -> "node-" + randomSuffix()            // 兜底随机
            )
            .map(Supplier::get)
            .filter(Objects::nonNull)
            .filter(s -> !s.isBlank())
            .findFirst()
            .orElse("node-" + randomSuffix());
    }

    /**
     * 文件命名:
     * part-{nodeId}-P{partition}-{startOffset}to{endOffset}-{timestamp}.snappy.parquet
     *
     * 多节点安全保证:
     * 1. nodeId不同 → 文件名不同 → 不冲突
     * 2. 同一节点的同一partition → offset范围不重叠 → 不冲突
     */
    public String generateFileName(int partition, long startOffset, long endOffset,
                                   String compression, String format) {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        return String.format("part-%s-P%d-%dto%d-%s.%s.%s",
            nodeId, partition, startOffset, endOffset, ts,
            compression.toLowerCase(), format.toLowerCase());
    }
}
```

## 四、Consumer Group 配置

```java
public Properties buildConsumerProperties() {
    Properties props = new Properties();

    // === 核心: 所有节点使用相同group.id ===
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-lake-writer");

    // 关闭自动提交，手动管理offset
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // === 分区分配策略 ===
    // Kafka 2.4+ 支持StickyAssignor; Kafka 1.x使用RoundRobinAssignor
    // StickyAssignor: Rebalance时尽量保持原有分配不变，减少分区迁移
    // 如果Kafka Broker是1.x版本, 改用RoundRobinAssignor
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        "org.apache.kafka.clients.consumer.StickyAssignor");
    // Kafka 1.x兼容写法:
    // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    //     "org.apache.kafka.clients.consumer.RoundRobinAssignor");

    // 会话超时: 节点失联多久后认为宕机
    // 30秒: 在快速检测和避免误判之间平衡
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

    // 心跳间隔: 通常为session.timeout的1/3
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");

    // poll间隔上限: 两次poll()之间的最大时间
    // 必须 > 最大flush耗时，否则会被踢出Consumer Group
    // 设为5分钟: 即使大Buffer flush也不会超时
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");

    // 单次poll最大记录数
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

    // 拉取数据配置: 批量拉取提升吞吐
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");      // 1MB
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");        // 最多等500ms

    return props;
}
```

## 五、Rebalance 处理

Rebalance 是多节点场景中**最关键的容错点**。当发生以下事件时会触发 Rebalance：
- 新节点加入 Consumer Group
- 节点退出（优雅关闭或宕机）
- Topic 的 Partition 数量变化
- 动态更新订阅的 Topic 列表

### 5.1 Rebalance 监听器

```java
public class SafeRebalanceListener implements ConsumerRebalanceListener {

    private final WriteBufferManager bufferManager;
    private final CheckpointManager checkpointManager;
    private final CrashRecoveryManager recoveryManager;
    private final Metrics metrics;

    /**
     * 分区即将被撤销 —— 在Rebalance开始前调用
     *
     * 核心逻辑: 把即将失去的分区的Buffer数据紧急flush到HDFS
     * 如果不flush, 这部分内存中的数据会丢失(虽然offset未提交, 新节点会重消费,
     * 但如果已经积累了几十万条, 重消费的代价很高)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        long start = System.currentTimeMillis();
        log.info("[Rebalance] 即将撤销 {} 个分区: {}", partitions.size(), partitions);
        metrics.counter("rebalance_revoked_total").increment();

        int successCount = 0;
        int failCount = 0;

        for (TopicPartition tp : partitions) {
            WriteBuffer buffer = bufferManager.getBuffer(tp);
            if (buffer == null || buffer.isEmpty()) {
                bufferManager.removeBuffer(tp);
                continue;
            }

            try {
                // 紧急flush: 无论是否达到阈值
                FlushResult result = buffer.forceFlush();
                if (result.isSuccess()) {
                    // flush成功 → 提交offset
                    consumer.commitSync(Map.of(
                        tp, new OffsetAndMetadata(buffer.getLastOffset() + 1)
                    ));
                    successCount++;
                    log.info("[Rebalance] flush成功: {} offset={}", tp, buffer.getLastOffset());
                }
            } catch (Exception e) {
                failCount++;
                // flush失败 → 不提交offset → 新Consumer会从上次committed offset重消费
                log.warn("[Rebalance] flush失败: {}, 数据将由新节点重消费", tp, e);
            }

            // 清理该分区的buffer和checkpoint
            bufferManager.removeBuffer(tp);
            checkpointManager.remove(tp);
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("[Rebalance] 撤销处理完成: 成功={}, 失败={}, 耗时={}ms",
            successCount, failCount, elapsed);
        metrics.timer("rebalance_revoke_duration").record(elapsed, TimeUnit.MILLISECONDS);
    }

    /**
     * 新分区已分配到本节点
     *
     * 核心逻辑:
     * 1. 检查是否有该分区的checkpoint(本节点之前消费过, crash后重新分配回来)
     * 2. 如有checkpoint → 执行crash恢复逻辑
     * 3. 为新分区创建Buffer
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        log.info("[Rebalance] 新分配 {} 个分区: {}", partitions.size(), partitions);
        metrics.counter("rebalance_assigned_total").increment();

        for (TopicPartition tp : partitions) {
            // 检查本地checkpoint
            Optional<Checkpoint> ckpt = checkpointManager.load(tp);
            if (ckpt.isPresent()) {
                // 有checkpoint → 执行恢复
                long seekOffset = recoveryManager.recoverPartition(ckpt.get());
                consumer.seek(tp, seekOffset);
                log.info("[Rebalance] 恢复checkpoint: {} → seek to {}", tp, seekOffset);
            }
            // else: 无checkpoint → 从Kafka committed offset消费(默认行为)

            // 为新分区创建WriteBuffer
            TopicSinkConfig config = configManager.getConfig(tp.topic());
            if (config != null) {
                bufferManager.createBuffer(tp, config);
            }
        }
    }
}
```

### 5.2 Rebalance 场景分析

#### 场景A: 扩容（新节点加入）

```
Before:  Node-1 [P0, P1, P2, P3, P4, P5]     (单节点消费全部6个分区)

Node-2 加入 → 触发Rebalance

Step 1: Node-1.onPartitionsRevoked([P3, P4, P5])
        → 紧急flush P3, P4, P5的Buffer
        → commit offset
        → 清理Buffer

Step 2: Kafka重新分配
        Node-1.onPartitionsAssigned([P0, P1, P2])
        Node-2.onPartitionsAssigned([P3, P4, P5])

After:   Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

影响: Rebalance期间约10-30秒消费暂停, 不丢数据
     使用StickyAssignor, P0/P1/P2留在Node-1, 减少迁移
```

#### 场景B: 优雅缩容（节点主动退出）

```
Before:  Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

对Node-2执行 kill -SIGTERM (优雅关闭)

Step 1: GracefulShutdown触发
        → flush所有Buffer (P3, P4, P5)
        → commit所有offset
        → consumer.close() → 向Broker发送LeaveGroup

Step 2: Kafka快速Rebalance (无需等session.timeout)
        Node-1.onPartitionsAssigned([P3, P4, P5])

After:   Node-1 [P0, P1, P2, P3, P4, P5]

影响: 几乎无数据延迟, Node-2已提前flush完毕
```

#### 场景C: 非优雅宕机（硬件故障/SIGKILL）

```
Before:  Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

Node-2 硬件故障, 突然失联

Step 1: 等待 session.timeout (30秒) → Broker检测到Node-2失联

Step 2: Kafka触发Rebalance
        Node-1.onPartitionsAssigned([P3, P4, P5])

Step 3: Node-1开始消费P3, P4, P5
        → 从Kafka最后committed offset开始消费
        → Node-2 Buffer中未flush的数据会被重新消费（重复）

After:   Node-1 [P0, P1, P2, P3, P4, P5]

影响: 30秒检测延迟 + 少量数据重复, 不丢数据
     重复量 = Node-2 Buffer中累积的未flush数据
```

#### 场景D: 节点数超过Partition数

```
Topic: orders (4 partitions), 部署6个节点

Node-1 [P0]    Node-2 [P1]    Node-3 [P2]    Node-4 [P3]    Node-5 [空闲]    Node-6 [空闲]

⚠️ Node-5和Node-6完全空闲, 不会分配到任何分区
   浪费资源, 但不会报错

建议: 节点数 ≤ 所有Topic的最小Partition数
     如需更高并行度, 先增加Kafka Partition数
```

## 六、多节点配置一致性

### 6.1 配置同步机制

所有节点从同一个配置中心拉取配置，无需节点间直接通信：

```
┌─────────────┐
│ Config Center│
│  (MySQL)     │
└──────┬──────┘
       │  所有节点读同一张表
  ┌────┼────┐
  │    │    │
  ▼    ▼    ▼
 N1   N2   N3    各自独立poll, 最终一致
```

**配置生效时序：**
- 各节点独立轮询（30秒间隔），最大不一致窗口 = 30秒
- 30秒内不同节点可能看到不同版本的配置
- 影响有限：最多导致个别节点多消费/少消费30秒的数据量

### 6.2 Topic订阅变更的多节点协调

```
配置变更: 新增topic "payments"

T=0s    管理员INSERT新配置
T=15s   Node-1 poll到新配置 → subscribe添加"payments"
        Kafka触发Rebalance
        payments的partition分配给Node-1
T=30s   Node-2 poll到新配置 → subscribe添加"payments"
        Kafka触发第二次Rebalance
        payments的partition在Node-1和Node-2之间重新分配
T=45s   Node-3 poll到新配置 → subscribe添加"payments"
        Kafka触发第三次Rebalance
        payments的partition在三个节点之间最终分配

最终状态: 所有节点都订阅了"payments", 分区均匀分配
```

**优化：多次Rebalance问题**

上述过程会触发多次Rebalance。优化方案：

```java
// 方案: 配置版本对齐 —— 所有节点等到同一个配置版本再更新订阅
public class VersionedConfigSync {

    /**
     * 检测到新配置后，不立即更新订阅
     * 而是等待一个宽限期(如60秒)，让所有节点都拉到新配置
     * 然后在对齐的时间点统一更新订阅
     */
    public void onConfigChanged(TopicSinkConfig newConfig) {
        long effectiveTime = newConfig.getUpdatedAt().toEpochMilli() + 60_000; // 60秒宽限
        long now = System.currentTimeMillis();

        if (now >= effectiveTime) {
            // 已过宽限期，立即生效
            applySubscriptionChange(newConfig);
        } else {
            // 未到生效时间，延迟执行
            long delay = effectiveTime - now;
            scheduler.schedule(() -> applySubscriptionChange(newConfig),
                delay, TimeUnit.MILLISECONDS);
            log.info("配置将在{}ms后生效, topic={}", delay, newConfig.getTopicName());
        }
    }
}
```

## 七、多节点监控

### 7.1 关键指标

```yaml
# 每个节点独立上报，按node标签区分
kafka_lake_writer_consumer_lag{node, topic, partition}       # 消费延迟(offset差)
kafka_lake_writer_records_consumed_total{node, topic}        # 消费总记录数
kafka_lake_writer_records_flushed_total{node, topic}         # 已flush总记录数
kafka_lake_writer_flush_duration_ms{node, topic}             # flush耗时
kafka_lake_writer_buffer_usage_ratio{node, topic}            # Buffer使用率
kafka_lake_writer_rebalance_count{node}                      # Rebalance次数
kafka_lake_writer_rebalance_revoke_duration_ms{node}         # Rebalance处理耗时
kafka_lake_writer_active_partitions{node}                    # 当前节点负责的分区数
kafka_lake_writer_flush_retry_total{node, topic}             # flush重试次数
kafka_lake_writer_flush_failure_total{node, topic}           # flush失败次数
```

### 7.2 告警规则

```yaml
groups:
  - name: kafka-lake-writer
    rules:
      # 消费延迟超过5分钟
      - alert: ConsumerLagHigh
        expr: max(kafka_lake_writer_consumer_lag) by (topic) > 300000
        for: 5m
        labels:
          severity: warning

      # 30分钟内Rebalance超过3次 → 可能有节点不稳定
      - alert: FrequentRebalance
        expr: increase(kafka_lake_writer_rebalance_count[30m]) > 3
        labels:
          severity: warning

      # 任一节点Buffer使用率超过80%
      - alert: BufferNearFull
        expr: kafka_lake_writer_buffer_usage_ratio > 0.8
        for: 2m
        labels:
          severity: critical

      # 节点间分区分配严重不均(最大/最小差距超过2倍)
      - alert: PartitionImbalance
        expr: >
          max(kafka_lake_writer_active_partitions) by (instance)
          / min(kafka_lake_writer_active_partitions) by (instance) > 2
        for: 10m
        labels:
          severity: warning

      # flush持续失败
      - alert: FlushFailure
        expr: increase(kafka_lake_writer_flush_failure_total[5m]) > 0
        labels:
          severity: critical
```

## 八、部署建议

### 8.1 容量规划

| 消息吞吐 | 建议节点数 | 单节点规格 | Kafka Partition数 |
|---------|----------|-----------|-----------------|
| <= 3W/s | 1 | 4C8G | >= 4 |
| 3W-6W/s | 1-2 | 4C8G | >= 8 |
| 6W-15W/s | 2-3 | 4C8G | >= 12 |
| 15W-30W/s | 4-6 | 4C8G | >= 24 |

### 8.2 K8s部署建议

```yaml
apiVersion: apps/v1
kind: Deployment       # 注意: 用Deployment而非StatefulSet
metadata:              # Consumer Group自动处理分区分配, 无需固定Pod标识
  name: kafka-lake-writer
spec:
  replicas: 2          # 节点数
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1          # 滚动更新: 先启动新Pod再关闭旧Pod
      maxUnavailable: 0    # 保证至少有replicas个Pod在运行
  template:
    spec:
      terminationGracePeriodSeconds: 120  # 给足优雅关闭时间
      containers:
        - name: kafka-lake-writer
          resources:
            requests:
              cpu: "2"
              memory: "6Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: checkpoint
              mountPath: /app/checkpoint    # checkpoint本地存储
      volumes:
        - name: checkpoint
          emptyDir: {}   # Pod重建后checkpoint丢失 → 触发从committed offset重消费(可接受)
```
