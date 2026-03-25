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
Phase 2: 将checkpoint信息写入本地磁盘
    │     checkpoint = {topic, partition, offsetRange, tmpFile, targetFile}
    ▼
Phase 3: rename临时文件 → 正式文件 (HDFS rename是原子操作)
    │
    ▼
Phase 4: commitSync offset到Kafka
    │
    ▼
Phase 5: 删除本地checkpoint文件
```

**每个Phase之间都可能crash，下面分析各crash场景的恢复策略。**

## 四、Crash场景与恢复策略

### 4.1 场景分类

| 编号 | Crash时机 | 数据在哪里 | HDFS状态 | Kafka Offset | 恢复策略 |
|------|----------|-----------|---------|--------------|---------|
| C-1 | poll后，入buffer前 | Kafka | 无文件 | 未提交 | 无需处理，自动重新poll |
| C-2 | buffer积累中，flush前 | 内存 | 无文件 | 未提交 | 无需处理，重新从committed offset消费 |
| C-3 | Phase 1执行中 | 部分写入tmp | 不完整tmp文件 | 未提交 | 检测到ckpt无完整tmp → 删除残留tmp → 回溯消费 |
| C-4 | Phase 1完成, Phase 2之前 | 完整tmp文件 | 完整tmp文件 | 未提交 | 无checkpoint → 清理孤儿tmp → 重新消费 |
| C-5 | Phase 2完成, Phase 3之前 | checkpoint + tmp | 完整tmp文件 | 未提交 | 读ckpt → 补完rename → seek到正确offset |
| C-6 | Phase 3完成, Phase 4之前 | checkpoint + 正式文件 | 正式文件已存在 | 未提交 | 读ckpt → 发现正式文件已存在 → seek到正确offset |
| C-7 | Phase 4完成, Phase 5之前 | 残留checkpoint | 正式文件已存在 | 已提交 | 读ckpt → 发现已完成 → 删除ckpt |

### 4.2 启动恢复流程

```
进程启动
   │
   ▼
扫描 ./checkpoint/ 目录
   │
   ├── 无checkpoint文件 ──► 正常启动，从Kafka committed offset消费
   │
   └── 有checkpoint文件 ──► 逐个处理
           │
           ▼
      读取checkpoint内容
           │
           ├── HDFS正式文件已存在？
           │      │
           │     Yes ──► 场景C-6/C-7: 文件已落盘
           │      │      seek到 endOffset+1
           │      │      删除checkpoint
           │      │
           │     No ──► HDFS临时文件存在？
           │             │
           │            Yes ──► 场景C-5: tmp完整但未rename
           │             │      执行rename: _tmp/xxx → 正式路径
           │             │      seek到 endOffset+1
           │             │      删除checkpoint
           │             │
           │            No ──► 场景C-3/C-4: 数据未完整落盘
           │                    seek到 startOffset (回溯重新消费)
           │                    删除checkpoint
           │
           ▼
      清理HDFS上所有 _tmp/ 目录下的残留文件
           │
           ▼
      恢复完成，开始正常消费循环
```

### 4.3 恢复代码设计

```java
public class CrashRecoveryManager {

    private final Path checkpointBaseDir;  // 本地: ./checkpoint/
    private final FileSystem hdfs;
    private final KafkaConsumer<String, String> consumer;

    /**
     * 进程启动时调用，在Consumer.poll()之前执行
     * @return 需要seek的offset映射表
     */
    public Map<TopicPartition, Long> recover() {
        Map<TopicPartition, Long> seekOffsets = new HashMap<>();

        for (File ckptFile : listAllCheckpoints()) {
            Checkpoint ckpt = Checkpoint.fromFile(ckptFile);
            TopicPartition tp = new TopicPartition(ckpt.getTopic(), ckpt.getPartition());

            try {
                if (hdfs.exists(new Path(ckpt.getTargetFilePath()))) {
                    // 正式文件已存在 → flush已完成，只需seek到正确位置
                    seekOffsets.put(tp, ckpt.getEndOffset() + 1);
                    log.info("[Recovery] 文件已落盘: {} → seek to {}", tp, ckpt.getEndOffset() + 1);

                } else if (hdfs.exists(new Path(ckpt.getTmpFilePath()))) {
                    // 临时文件存在 → 补完rename
                    hdfs.rename(new Path(ckpt.getTmpFilePath()), new Path(ckpt.getTargetFilePath()));
                    seekOffsets.put(tp, ckpt.getEndOffset() + 1);
                    log.info("[Recovery] 补完rename: {} → seek to {}", tp, ckpt.getEndOffset() + 1);

                } else {
                    // 临时文件也不存在 → 数据未落盘，需要回溯
                    seekOffsets.put(tp, ckpt.getStartOffset());
                    log.info("[Recovery] 数据未落盘: {} → 回溯到 {}", tp, ckpt.getStartOffset());
                }
            } catch (IOException e) {
                // HDFS不可用 → 保守处理，回溯到startOffset
                seekOffsets.put(tp, ckpt.getStartOffset());
                log.warn("[Recovery] HDFS检测失败: {} → 回溯到 {}", tp, ckpt.getStartOffset(), e);
            }

            // 删除已处理的checkpoint
            ckptFile.delete();
        }

        // 清理HDFS上所有_tmp目录残留
        cleanOrphanTmpFiles();

        return seekOffsets;
    }

    /**
     * 清理所有配置路径下的_tmp目录
     */
    private void cleanOrphanTmpFiles() {
        for (TopicSinkConfig config : configManager.getAllActive()) {
            try {
                Path tmpDir = new Path(config.getSinkPath(), "_tmp");
                if (hdfs.exists(tmpDir)) {
                    FileStatus[] tmpFiles = hdfs.listStatus(tmpDir);
                    for (FileStatus f : tmpFiles) {
                        hdfs.delete(f.getPath(), false);
                        log.info("[Recovery] 清理残留临时文件: {}", f.getPath());
                    }
                }
            } catch (IOException e) {
                log.warn("[Recovery] 清理临时文件失败: {}", config.getSinkPath(), e);
            }
        }
    }
}
```

## 五、HDFS/OSS 不可用容错

### 5.1 重试与退避策略

```
第1次失败 → 等待 2秒 → 重试
第2次失败 → 等待 4秒 → 重试
第3次失败 → 等待 8秒 → 重试
第3次仍失败 → 进入降级模式
```

### 5.2 降级模式

当HDFS/OSS持续不可用时：

1. **暂停消费**：调用`consumer.pause(partitions)`，停止poll该topic的数据
2. **数据保留**：Buffer中的数据保留在内存中，不丢弃
3. **触发告警**：发送CRITICAL级别告警
4. **自动检测恢复**：每60秒检测HDFS是否恢复
5. **自动恢复消费**：HDFS恢复后，先flush积压Buffer，再调用`consumer.resume(partitions)`

```java
public class ResilientFlushExecutor {

    private static final int MAX_RETRIES = 3;

    public FlushResult flushWithRetry(WriteBuffer buffer) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                return doFlush(buffer);
            } catch (IOException e) {
                attempt++;
                log.warn("Flush失败 [{}/{}], topic={}, partition={}",
                    attempt, MAX_RETRIES, buffer.getTopic(), buffer.getPartition(), e);
                if (attempt < MAX_RETRIES) {
                    sleepMs(2000L * (1L << (attempt - 1)));  // 2s, 4s, 8s
                }
            }
        }

        // 超过重试上限: 进入降级模式
        enterDegradedMode(buffer);
        return FlushResult.FAILED;
    }

    private void enterDegradedMode(WriteBuffer buffer) {
        // 暂停该分区的消费
        Set<TopicPartition> tps = buffer.getTopicPartitions();
        consumerPool.pausePartitions(tps);

        // 触发告警
        metrics.counter("flush_failure_total",
            "topic", buffer.getTopic()).increment();
        alertManager.fire(AlertLevel.CRITICAL,
            String.format("HDFS写入失败, topic=%s, partition=%s, bufferSize=%d",
                buffer.getTopic(), buffer.getPartition(), buffer.size()));

        // 启动恢复检测定时任务
        scheduleRecoveryCheck(buffer, tps);
    }

    private void scheduleRecoveryCheck(WriteBuffer buffer, Set<TopicPartition> tps) {
        scheduler.scheduleAtFixedRate(() -> {
            if (isStorageAvailable()) {
                try {
                    FlushResult result = doFlush(buffer);
                    if (result.isSuccess()) {
                        consumerPool.resumePartitions(tps);
                        log.info("存储恢复, 自动resume消费: {}", tps);
                        // 取消检测任务（通过返回的ScheduledFuture.cancel）
                    }
                } catch (Exception e) {
                    log.debug("存储仍不可用, 继续等待...");
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }
}
```

## 六、Graceful Shutdown（优雅关闭）

进程收到SIGTERM信号时的关闭流程：

```
SIGTERM / SIGINT 信号
    │
    ▼
1. 设置 running = false, 停止poll循环
    │
    ▼
2. 遍历所有WriteBuffer, 执行紧急flush
    │  不管是否达到阈值，所有Buffer数据都flush到HDFS
    ▼
3. 提交所有已flush的offset
    │
    ▼
4. 关闭Consumer (触发LeaveGroup, Kafka快速rebalance)
    │
    ▼
5. 关闭HDFS FileSystem连接
    │
    ▼
6. 关闭线程池 (先shutdown, 等待30秒, 再shutdownNow)
    │
    ▼
进程退出
```

```java
@Component
public class GracefulShutdownHook {

    @PreDestroy
    public void shutdown() {
        log.info("收到关闭信号，开始优雅关闭...");

        // 1. 停止消费循环
        consumerPool.stopPolling();

        // 2. flush所有buffer
        int flushed = bufferManager.flushAll();
        log.info("紧急flush完成, 共flush {} 个buffer", flushed);

        // 3. 提交offset
        consumerPool.commitAllOffsets();

        // 4. 关闭consumer
        consumerPool.close();

        // 5. 关闭存储连接
        storageAdapter.close();

        // 6. 关闭线程池
        writerPool.shutdown();
        try {
            if (!writerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                writerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            writerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("优雅关闭完成");
    }
}
```

## 七、数据去重支持

### 7.1 幂等写入检测

利用文件名中的offset范围实现幂等检测：

```java
/**
 * 启动时扫描目标目录，构建已写入offset区间索引
 * flush前检查：如果该offset范围已有文件，跳过写入
 */
public class IdempotentWriteChecker {

    // key: TopicPartition, value: 已写入的offset区间 (startOffset → endOffset)
    private final Map<TopicPartition, TreeMap<Long, Long>> writtenRanges = new ConcurrentHashMap<>();

    public void buildIndex(String sinkPath) {
        // 扫描目录下的parquet/csv文件，从文件名解析offset范围
        // 文件名: part-pod01-P3-580000to582390-20260322140530.snappy.parquet
        for (FileStatus file : hdfs.listStatus(new Path(sinkPath))) {
            String name = file.getPath().getName();
            OffsetRange range = OffsetRange.parseFromFileName(name);
            if (range != null) {
                writtenRanges
                    .computeIfAbsent(range.topicPartition(), k -> new TreeMap<>())
                    .put(range.startOffset(), range.endOffset());
            }
        }
    }

    public boolean isAlreadyWritten(TopicPartition tp, long startOffset, long endOffset) {
        TreeMap<Long, Long> ranges = writtenRanges.get(tp);
        if (ranges == null) return false;
        Map.Entry<Long, Long> floor = ranges.floorEntry(startOffset);
        return floor != null && floor.getValue() >= endOffset;
    }
}
```

### 7.2 下游去重建议

对于Hive表，建议使用分区覆盖写入：
```sql
-- 定期compaction: 对同一分区的多个小文件合并去重
INSERT OVERWRITE TABLE orders PARTITION(dt='2026-03-22', hour='14')
SELECT DISTINCT * FROM orders WHERE dt='2026-03-22' AND hour='14';
```
