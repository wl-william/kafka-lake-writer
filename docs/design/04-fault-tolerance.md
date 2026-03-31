# Kafka-Lake-Writer 容错机制设计

## 一、数据安全语义

**设计目标：At-Least-Once（至少一次）**

- 数据不丢失：任何crash场景下，已消费的数据最终都会写入HDFS/OSS
- 可接受少量重复：crash恢复时可能产生少量重复数据，可通过下游去重处理
- 不丢数据优先于不重复数据：对数据仓库场景，丢失不可接受，重复可通过`DISTINCT`或分区覆盖处理

## 二、Offset提交策略

### 2.1 核心原则：先落盘再提交

```
  ✗ 错误方式: commit offset → flush to HDFS
    风险: offset提交成功但flush失败 → 数据丢失

  ✓ 正确方式: flush to HDFS → commit offset
    风险: flush成功但commit失败 → 重启后重复消费（可接受）
```

### 2.2 手动Offset管理

```java
// Consumer配置: 关闭自动提交
props.put("enable.auto.commit", "false");

// 每次成功flush后，手动提交该batch的offset
Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
offsets.put(
    new TopicPartition(topic, partition),
    new OffsetAndMetadata(lastOffset + 1)  // +1: 下次从这里开始消费
);
consumer.commitSync(offsets);
```

## 三、Flush安全写入流程（五阶段提交）

```
Phase 1: 数据序列化，写入HDFS/OSS临时文件 (_tmp目录)
    │
    ▼
Phase 2: 将checkpoint写入HDFS/OSS（RemoteCheckpointManager）
    │     路径: {remoteBaseDir}/{topic}/P{partition}.ckpt
    │     内容: {topic, partition, offsetRange, tmpFile, targetFile, nodeId}
    │     ※ 存储在共享存储上，任意节点接管后均可读取
    ▼
Phase 3: rename临时文件 → 正式文件
    │     HDFS: 服务端原子操作
    │     OSS:  copy + delete（非原子，见下文OSS特殊处理）
    ▼
Phase 4: commitSync offset到Kafka（在ConsumerWorker线程执行）
    │
    ▼
Phase 5: 删除HDFS/OSS上的checkpoint文件
```

**每个Phase之间都可能crash，下面分析各crash场景的恢复策略。**

### 3.1 OSS rename非原子处理

OSS的`AliyunOSSFileSystem.rename()`是copy + delete，不是原子操作。如果crash发生在copy完成但delete之前，重启时target和tmp同时存在。

`OssStorageAdapter`重写`rename()`以确保幂等：

```java
@Override
public boolean rename(String srcPath, String dstPath) throws IOException {
    // target已存在 → copy已完成，只需删除source
    if (exists(dstPath)) {
        delete(srcPath);
        return true;
    }
    return super.rename(srcPath, dstPath);
}
```

## 四、Crash场景与恢复策略

### 4.1 场景分类

| 编号 | Crash时机 | checkpoint状态 | HDFS状态 | Kafka Offset | 恢复策略 |
|------|----------|--------------|---------|--------------|---------|
| C-1 | poll后，入buffer前 | 无 | 无文件 | 未提交 | 无需处理，自动重新poll |
| C-2 | buffer积累中，flush前 | 无 | 无文件 | 未提交 | 从committed offset重消费 |
| C-3 | Phase 1执行中 | 无 | 不完整tmp | 未提交 | 无checkpoint → 回溯到committed offset重消费 |
| C-4 | Phase 1完成, Phase 2之前 | 无 | 完整tmp | 未提交 | 无checkpoint → 回溯到committed offset；孤儿tmp留在_tmp目录 |
| C-5 | Phase 2完成, Phase 3之前 | 有（远端） | 完整tmp，无target | 未提交 | 读远端ckpt → 补完rename → seek到endOffset+1 |
| C-6 | Phase 3完成, Phase 4之前 | 有（远端） | target存在 | 未提交 | 读远端ckpt → 发现target已存在 → seek到endOffset+1 |
| C-7 | Phase 4完成, Phase 5之前 | 有（远端） | target存在 | 已提交 | 读远端ckpt → 发现已完成 → seek到endOffset+1 → 删除ckpt |

### 4.2 恢复触发时机：onPartitionsAssigned

恢复**不在进程启动时执行**，而是在Kafka分配分区后的`onPartitionsAssigned()`回调中执行。这保证了`consumer.seek()`在分区实际分配后才调用，且对所有节点（包括跨节点接管）均有效。

```
Kafka触发Rebalance
    │
    ▼
SafeRebalanceListener.onPartitionsAssigned(partitions)
    │
    ├── for each tp in partitions:
    │       RemoteCheckpointManager.load(tp)      ← 读HDFS/OSS，任意节点均可访问
    │           │
    │           ├── 无checkpoint（C-1/C-2）
    │           │      → 不seek，从Kafka committed offset消费
    │           │
    │           └── 有checkpoint
    │                  │
    │                  ├── target文件存在（C-6/C-7）
    │                  │      → seek to endOffset+1
    │                  │      → idempotentChecker.recordWritten(start, end)  ← 防重复写
    │                  │
    │                  ├── tmp文件存在（C-5）
    │                  │      → storage.rename(tmp, target)
    │                  │      → seek to endOffset+1
    │                  │      → idempotentChecker.recordWritten(start, end)
    │                  │
    │                  └── 无文件（C-3/C-4）
    │                         → seek to startOffset（回溯重消费）
    │
    └── createBuffer(tp, config)
```

### 4.3 OffsetOutOfRangeException处理

当checkpoint中记录的offset已被Kafka日志清理（log retention过期），`consumer.seek()`后的下次`poll()`会抛出`OffsetOutOfRangeException`。

处理策略：**seek到该分区的earliest可用offset（at-least-once）**。

```
ConsumerWorker.run() poll循环:
    try {
        records = consumer.poll(...)
    } catch (OffsetOutOfRangeException e) {
        → consumer.beginningOffsets(e.partitions())
        → 对每个受影响分区 seek to earliest
        → continue（下次poll从earliest开始）
    }
```

重消费可能产生少量重复数据，但不丢失数据，符合At-Least-Once语义。

## 五、HDFS/OSS 持续不可用容错

### 5.1 三级重试策略

```
第1次失败 → 等待 2秒 → 重试
第2次失败 → 等待 4秒 → 重试
第3次失败 → 等待 8秒 → 重试
第3次仍失败 → 视为"存储持续不可用"，进入分区暂停模式
```

### 5.2 分区暂停 + 自动探测恢复

当flush全部重试失败后，**不丢弃数据，不退出进程**，而是：

1. 将失败的`TopicPartition`加入`failedFlushSignals`队列（flush线程 → consumer线程）
2. consumer线程在下次poll前调用`drainFlushFailureSignals()`：
   - 调用`consumer.pause(tp)`暂停该分区拉取
   - 记入`pausedDueToFailure`集合
3. `StorageHealthChecker`在每次poll循环中探测存储可用性（内部30秒限速）
4. 探测成功 → `consumer.resume(pausedDueToFailure)` → 继续从last committed offset消费

```
存储故障时机线：

flush失败(3次) → consumer.pause(tp) → 停止消费
       │
       │  [存储恢复前]
       │   StorageHealthChecker.probe() 每30s探测
       │   → 失败：LOG DEBUG，继续等待
       │
       │  [存储恢复后]
       ▼
StorageHealthChecker.probe() → 成功 → LOG INFO "Storage recovered"
       │
       ▼
consumer.resume(pausedPartitions) → 继续从committed offset消费
       │
       ├── offset仍有效 → 正常重消费 ✓
       └── offset已过期 → OffsetOutOfRangeException → seek to earliest ✓
```

**关键保证**：暂停期间不提交任何更高的offset，确保存储恢复后Kafka能重新投递失败批次的数据。

### 5.3 BackpressureController内存保护

```
max-total-bytes: 3GB
暂停阈值: 90% (2.7GB) → canConsume() = false → 停止poll
超限告警: > 100% → LOG WARN "CRITICAL: buffer exceeds hard limit"
```

消费线程睡眠时间随压力自适应：
- `canConsume() = false && !isCritical()` → sleep 100ms
- `canConsume() = false && isCritical()` → sleep 500ms（减少CPU自旋）

## 六、Graceful Shutdown（优雅关闭）

进程收到SIGTERM信号时的关闭流程：

```
SIGTERM / SIGINT 信号
    │
    ▼
1. consumerPool.stopPolling()    → 设置running=false，wakeup()所有consumer
    │
    ▼
2. 遍历所有WriteBuffer，执行紧急flush
    │  Phases 1-3：写tmp → 保存远端checkpoint → rename
    │  ※ 此处不删checkpoint，留作安全网（Phase 5由commitSync后执行）
    ▼
3. consumerPool.commitAllOffsets()   → commitSync所有分区
    │
    ▼
4. consumerPool.close()              → 发送LeaveGroup，触发快速Rebalance
    │
    ▼
5. storage.close()
    │
    ▼
进程退出
```

## 七、幂等写入（去重）

### 7.1 基于远端Checkpoint的去重（无listFiles开销）

**旧方案问题**：`IdempotentWriteChecker.buildIndex()`调用`listFiles(sinkPath)`扫描目录，代价随文件数线性增长，对NameNode/OSS造成压力。

**新方案**：checkpoint恢复时直接填充去重索引，无需扫描目录。

```
onPartitionsAssigned(tp):
    checkpoint = remoteCheckpointMgr.load(tp)
    if checkpoint存在 && 恢复后seekTo == endOffset+1:
        // 说明文件已写完，直接记录，不再扫描目录
        idempotentChecker.recordWritten(tp, start, end)

flush前:
    idempotentChecker.isAlreadyWritten(tp, start, end)?
        → true:  return FlushResult.skipped()   // 跳过，不重复写
        → false: doFlush() → recordWritten()
```

`isAlreadyWritten()`的判断逻辑（TreeMap floorEntry）：

```
writtenRanges[P0] = TreeMap{ 100→199 }

查询[100, 199]: floorEntry(100)=entry(100,199), 199≥199 → true  跳过 ✓
查询[120, 170]: floorEntry(120)=entry(100,199), 199≥170 → true  跳过 ✓
查询[100, 250]: floorEntry(100)=entry(100,199), 199≥250 → false 写入 ✓
```

### 7.2 下游去重建议

对于Hive表，建议使用分区覆盖写入：
```sql
-- 定期compaction: 对同一分区的多个小文件合并去重
INSERT OVERWRITE TABLE orders PARTITION(dt='2026-03-22', hour='14')
SELECT DISTINCT * FROM orders WHERE dt='2026-03-22' AND hour='14';
```
