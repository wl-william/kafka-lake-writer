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
│  Checkpoint目录（所有节点共享，Phase 2写入，Phase 5删除）:       │
│  /lake-writer-checkpoints/orders/P0.ckpt                        │
│  /lake-writer-checkpoints/orders/P3.ckpt                        │
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
}
```

## 四、Consumer Group 配置

```java
// 关键参数
props.put(ConsumerConfig.GROUP_ID_CONFIG,              "kafka-lake-writer");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,    "false");
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,    "30000");   // 30秒检测宕机
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");   // 10秒心跳
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,  "300000");  // 5分钟 > 最大flush时间
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,      "5000");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,     "latest");  // 无committed offset时从最新消费
```

## 五、Rebalance 处理

Rebalance 是多节点场景中**最关键的容错点**。当发生以下事件时会触发 Rebalance：
- 新节点加入 Consumer Group
- 节点退出（优雅关闭或宕机）
- Topic 的 Partition 数量变化
- 动态更新订阅的 Topic 列表

### 5.1 onPartitionsRevoked — 分区撤销前紧急flush

```
核心逻辑: 把即将失去的分区的Buffer数据紧急flush到HDFS/OSS
如果flush成功 → commitSync offset → 删除远端checkpoint
如果flush失败 → 不提交offset → 新节点接管后通过远端checkpoint恢复
```

流程：
```
onPartitionsRevoked(partitions):
    for each tp:
        if buffer不为空:
            Phases 1-3: flush (write tmp → save remote ckpt → rename)
            if success:
                Phase 4: commitSync
                Phase 5: checkpointMgr.delete(tp)   ← 删远端checkpoint
            else:
                留checkpoint在HDFS/OSS → 新owner会读到并恢复
        bufferManager.removeBuffer(tp)
```

### 5.2 onPartitionsAssigned — 分区接管与崩溃恢复

**关键改进**：Checkpoint存储在HDFS/OSS（`RemoteCheckpointManager`），任意节点接管都能读取，不再局限于"本节点之前消费过"的情况。

```
onPartitionsAssigned(partitions):
    for each tp:
        // 从HDFS/OSS读取checkpoint（任意节点均可访问）
        ckpt = remoteCheckpointMgr.load(tp)

        if ckpt存在:
            seekTo = CrashRecoveryManager.recoverPartition(ckpt)
            consumer.seek(tp, seekTo)

            // 若文件已写完（C-5/C-6/C-7），填充去重索引，无需listFiles扫描
            if seekTo == ckpt.endOffset + 1:
                idempotentChecker.recordWritten(tp, ckpt.start, ckpt.end)

        createBuffer(tp, config)
```

## 六、Rebalance 场景分析

#### 场景A: 扩容（新节点加入）

```
Before:  Node-1 [P0, P1, P2, P3, P4, P5]

Node-2 加入 → 触发Rebalance

Step 1: Node-1.onPartitionsRevoked([P3, P4, P5])
        → 紧急flush P3, P4, P5的Buffer → commit offset → 清理Buffer

Step 2: Kafka重新分配
        Node-1.onPartitionsAssigned([P0, P1, P2])
        Node-2.onPartitionsAssigned([P3, P4, P5])

After:   Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

影响: Rebalance期间约10-30秒消费暂停，不丢数据
```

#### 场景B: 优雅缩容（节点主动退出 SIGTERM）

```
Before:  Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

对Node-2执行 kill -SIGTERM (优雅关闭)

Step 1: GracefulShutdownHook触发
        → flush所有Buffer (P3, P4, P5)
        → commitAllOffsets
        → consumer.close() → 向Broker发送LeaveGroup

Step 2: Kafka快速Rebalance (无需等session.timeout)
        Node-1.onPartitionsAssigned([P3, P4, P5])
        → 无远端checkpoint（已在Phase 5删除）→ 从committed offset消费

After:   Node-1 [P0, P1, P2, P3, P4, P5]

影响: 几乎无数据延迟，Node-2已提前flush完毕
```

#### 场景C: 非优雅宕机（SIGKILL / 硬件故障）

```
Before:  Node-1 [P0, P1, P2]    Node-2 [P3, P4, P5]

Node-2 突然宕机（crash发生在某个Phase）

Step 1: 等待 session.timeout (30秒) → Broker检测到Node-2失联

Step 2: Kafka触发Rebalance
        Node-1.onPartitionsAssigned([P3, P4, P5])

Step 3: Node-1执行远端Checkpoint恢复（以P3为例）
        ├── 无checkpoint（crash在Phase 1/2之前）
        │      → 从Kafka committed offset消费
        │      → at-least-once: P3未提交的数据会重消费 ✓
        │
        ├── 有checkpoint + target文件存在（crash在Phase 3之后）
        │      → idempotentChecker.recordWritten(P3, start, end)
        │      → seek到endOffset+1，跳过已写范围 ✓
        │
        └── 有checkpoint + tmp文件存在（crash在Phase 2~3之间）
               → 补完rename(tmp, target)
               → seek到endOffset+1 ✓

After:   Node-1 [P0, P1, P2, P3, P4, P5]

影响: 30秒检测延迟；Checkpoint存储在HDFS/OSS，跨节点恢复完整可靠
```

> **与旧方案的关键区别**：
> 旧方案Checkpoint在本地磁盘，Node-1接管Node-2的分区时无法读到checkpoint，
> 只能从committed offset重消费，Phase 3之后的crash会产生重复文件。
> 新方案Checkpoint在共享存储，任意节点均可读取，消除重复写入风险。

#### 场景D: 节点数超过Partition数

```
Topic: orders (4 partitions), 部署6个节点

Node-1 [P0]  Node-2 [P1]  Node-3 [P2]  Node-4 [P3]  Node-5 [空闲]  Node-6 [空闲]

⚠️ Node-5和Node-6完全空闲，不会分配到任何分区，浪费资源

建议: 节点数 ≤ 所有Topic的最小Partition数
     如需更高并行度，先增加Kafka Partition数
```

## 七、多节点配置一致性

### 7.1 配置同步机制

所有节点从同一个配置中心拉取配置，无需节点间直接通信：
- 各节点独立轮询（30秒间隔），最大不一致窗口 = 30秒
- 新增topic时，各节点在60秒宽限期后统一生效（`VersionedConfigSync`），减少多次Rebalance

## 八、多节点监控

### 8.1 关键指标

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
kafka_lake_writer_storage_probe_failure{node}                # 存储探测失败次数（新增）
kafka_lake_writer_paused_partitions{node}                    # 因存储故障暂停的分区数（新增）
```

### 8.2 告警规则

```yaml
groups:
  - name: kafka-lake-writer
    rules:
      - alert: ConsumerLagHigh
        expr: max(kafka_lake_writer_consumer_lag) by (topic) > 300000
        for: 5m
        labels:
          severity: warning

      - alert: FrequentRebalance
        expr: increase(kafka_lake_writer_rebalance_count[30m]) > 3
        labels:
          severity: warning

      - alert: BufferNearFull
        expr: kafka_lake_writer_buffer_usage_ratio > 0.8
        for: 2m
        labels:
          severity: critical

      - alert: FlushFailure
        expr: increase(kafka_lake_writer_flush_failure_total[5m]) > 0
        labels:
          severity: critical

      - alert: PartitionsPaused
        expr: kafka_lake_writer_paused_partitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "分区因存储故障暂停消费，需确认HDFS/OSS状态"
```

## 九、部署建议

### 9.1 容量规划

| 消息吞吐 | 建议节点数 | 单节点规格 | Kafka Partition数 |
|---------|----------|-----------|-----------------|
| <= 3W/s | 1 | 4C8G | >= 4 |
| 3W-6W/s | 1-2 | 4C8G | >= 8 |
| 6W-15W/s | 2-3 | 4C8G | >= 12 |
| 15W-30W/s | 4-6 | 4C8G | >= 24 |

### 9.2 K8s部署建议

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-lake-writer
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      terminationGracePeriodSeconds: 120
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
            - name: CHECKPOINT_REMOTE_DIR
              value: "hdfs://namenode:8020/lake-writer-checkpoints"
              # OSS示例: "oss://my-bucket/lake-writer-checkpoints"
            - name: STORAGE_PROBE_PATH
              value: "/"
            - name: STORAGE_PROBE_INTERVAL_SEC
              value: "30"
          # ※ 不再需要本地checkpoint volume
          #    Checkpoint现在存储在HDFS/OSS，Pod重建后自动从远端读取
```

> **与旧版本部署的关键差异**：
> 旧版本需要挂载本地checkpoint目录（emptyDir或PVC），Pod重建后checkpoint丢失。
> 新版本Checkpoint写入HDFS/OSS，无本地存储依赖，Pod可随时重建而不影响恢复能力。
