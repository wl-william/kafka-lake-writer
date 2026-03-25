# Kafka-Lake-Writer 性能设计

## 一、性能目标

| 指标 | 目标 | 条件 |
|------|------|------|
| 消费吞吐 | >= 60,000 records/s | 单节点4C8G，JSON消息平均500B |
| 原始数据吞吐 | >= 30 MB/s | 60000 × 500B |
| Parquet写入吞吐 | >= 8-12 MB/s | 压缩比约3:1~4:1 |
| 端到端延迟 | <= 10分钟 | 从Kafka到HDFS文件可查询 |
| GC停顿 | < 200ms | P99 |

## 二、性能瓶颈分析

```
  Kafka Poll   →   JSON Parse   →   Buffer Write   →   File Flush
  (网络IO)         (CPU密集)         (内存操作)         (IO密集)

  吞吐上限:        吞吐上限:          吞吐上限:          吞吐上限:
  ~200K/s          ~80K/s            ~500K/s            ~30K/s(Parquet)
  (取决于网络)      (取决于CPU)        (内存够即可)       (取决于磁盘/网络IO)

  结论: JSON解析 和 Parquet写入 是两个主要瓶颈
```

## 三、各层优化策略

### 3.1 消费层优化

```java
// 批量拉取，减少网络RT
props.put("max.poll.records", "5000");         // 每次poll 5000条
props.put("fetch.min.bytes", "1048576");       // 至少攒1MB数据再返回
props.put("fetch.max.wait.ms", "500");         // 最多等500ms
props.put("receive.buffer.bytes", "1048576");  // 接收缓冲区1MB

// 多Consumer并行: 建议consumer数 = CPU核数 或 = partition数(取小)
// 4C机器 → 4个Consumer线程
```

### 3.2 JSON解析优化

```java
/**
 * 使用fastjson2的高性能解析模式
 */
public class FastJsonParser {

    // 1. 预编译Schema，避免每条消息重新反射
    private final Map<String, FieldDef[]> schemaCache = new ConcurrentHashMap<>();

    /**
     * 批量解析: 比逐条解析快约30%
     * 原因: JIT编译优化 + CPU分支预测 + L1缓存命中率提升
     */
    public List<Object[]> parseBatch(List<ConsumerRecord<String, String>> records,
                                     FieldDef[] schema) {
        List<Object[]> result = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            Object[] row = parseToRow(record.value(), schema);
            if (row != null) {
                result.add(row);
            }
        }
        return result;
    }

    /**
     * 解析为Object数组(比Map快，减少对象创建)
     */
    private Object[] parseToRow(String json, FieldDef[] schema) {
        try {
            JSONObject obj = JSON.parseObject(json);
            Object[] row = new Object[schema.length];
            for (int i = 0; i < schema.length; i++) {
                Object val = obj.get(schema[i].getName());
                row[i] = castValue(val, schema[i].getType());
            }
            return row;
        } catch (Exception e) {
            // 解析失败: 记录到错误日志/死信队列，不阻塞主流程
            metrics.counter("parse_error_total").increment();
            return null;
        }
    }

    // JDK 8兼容: 使用if-else替代switch表达式
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
}
```

### 3.3 Buffer层优化

```java
/**
 * 双Buffer交替: 消费和flush并行
 *
 * 问题: 如果Buffer在flush过程中，消费线程只能等待 → 浪费消费能力
 * 方案: 双Buffer交替，Buffer-A在flush时，新数据写入Buffer-B
 */
public class DoubleWriteBuffer {

    private volatile WriteBuffer active;   // 正在接收数据的Buffer
    private volatile WriteBuffer flushing; // 正在flush的Buffer
    private final ReentrantLock swapLock = new ReentrantLock();

    /**
     * 写入数据: 写入active buffer (无锁, 因为单partition单consumer线程)
     */
    public void append(Object[] row, long offset) {
        active.append(row, offset);
    }

    /**
     * 触发flush: 交换active和flushing
     */
    public WriteBuffer swapForFlush() {
        swapLock.lock();
        try {
            WriteBuffer toFlush = active;
            active = flushing != null ? flushing : new WriteBuffer(config);
            flushing = null;
            return toFlush;
        } finally {
            swapLock.unlock();
        }
    }

    /**
     * flush完成后回收buffer
     */
    public void recycleFlushed(WriteBuffer buffer) {
        buffer.clear();
        swapLock.lock();
        try {
            flushing = buffer;  // 回收用于下次交替
        } finally {
            swapLock.unlock();
        }
    }
}
```

### 3.4 Parquet写入优化

```java
/**
 * Parquet写入性能关键参数
 */
public ParquetWriter<Group> createWriter(Path filePath, MessageType schema,
                                         Configuration conf) throws IOException {
    return ExampleParquetWriter.builder(filePath)
        .withType(schema)
        .withConf(conf)
        // RowGroup大小: 128MB, HDFS block对齐，减少读取时的IO次数
        .withRowGroupSize(128 * 1024 * 1024)
        // 页大小: 1MB, 平衡列式编码效率和内存占用
        .withPageSize(1024 * 1024)
        // 压缩: Snappy (CPU开销最低, 压缩比约3:1)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        // 字典编码页大小
        .withDictionaryPageSize(1024 * 1024)
        // 开启字典编码 (对低基数字段效果显著)
        .withDictionaryEncoding(true)
        // 写入模式: OVERWRITE (临时文件，不会覆盖正式文件)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
}
```

### 3.5 写入线程池设计

```java
/**
 * 独立写入线程池: 消费线程不直接做IO操作
 *
 * 消费线程: 负责poll + parse + buffer
 * 写入线程: 负责serialize + write + checkpoint + commit
 */
@Configuration
public class WriterThreadPoolConfig {

    @Bean("fileWriterPool")
    public ThreadPoolExecutor fileWriterPool() {
        return new ThreadPoolExecutor(
            2,                              // 核心线程数: 2 (4C机器, 留2C给消费)
            4,                              // 最大线程数: 4
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(64),   // 队列容量: 最多64个待flush任务
            new ThreadFactoryBuilder().setNameFormat("file-writer-%d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy()  // 队列满时: 消费线程自己flush(背压)
        );
    }
}
```

## 四、内存管理

### 4.1 内存分配规划（8GB总量）

```
┌──────────────────────────────────────────────┐
│                 8GB 总内存                     │
├──────────────────────────────────────────────┤
│                                              │
│  JVM Heap: 5GB (-Xmx5g -Xms5g)             │
│  ├── WriteBuffer数据: ~3GB                   │
│  │   每个partition buffer ~ 150-300MB        │
│  │   假设10个partition → 1.5-3GB             │
│  ├── JSON解析临时对象: ~500MB                │
│  ├── Parquet编码缓冲: ~500MB                 │
│  └── 框架/其他: ~1GB                         │
│                                              │
│  堆外内存: ~2GB                              │
│  ├── Kafka网络接收缓冲                       │
│  ├── HDFS DFSClient缓冲                     │
│  └── 操作系统页缓存                          │
│                                              │
│  预留: ~1GB                                  │
│  └── 操作系统 + 其他进程                     │
│                                              │
└──────────────────────────────────────────────┘
```

### 4.2 背压控制

```java
/**
 * 当Buffer总内存超过阈值时，暂停Consumer poll
 * 防止OOM
 */
public class BackpressureController {

    private final long maxBufferBytes;  // 默认3GB
    private final AtomicLong currentBufferBytes = new AtomicLong(0);

    /**
     * 写入buffer前检查背压
     * @return true = 可以继续写入, false = 需要暂停
     */
    public boolean tryAcquire(long bytes) {
        long current = currentBufferBytes.addAndGet(bytes);
        if (current > maxBufferBytes) {
            // 超过阈值: 通知消费线程暂停poll
            currentBufferBytes.addAndGet(-bytes);
            return false;
        }
        return true;
    }

    public void release(long bytes) {
        currentBufferBytes.addAndGet(-bytes);
    }

    /**
     * 消费线程的poll循环中集成背压检查
     */
    public void pollLoop() {
        while (running) {
            if (!backpressure.canConsume()) {
                // 背压触发: 暂停poll，等待flush释放内存
                Thread.sleep(100);  // 短暂等待
                continue;
            }
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            // process records...
        }
    }
}
```

## 五、JVM调优参数

```bash
# JDK 1.8 调优参数
JAVA_OPTS="
  -Xms5g -Xmx5g                          # 堆内存固定5GB,避免扩容停顿
  -XX:+UseG1GC                            # G1垃圾回收器(JDK 8支持)
  -XX:MaxGCPauseMillis=200                # 目标GC停顿 < 200ms
  -XX:G1HeapRegionSize=16m                # Region大小16MB(大对象友好)
  -XX:InitiatingHeapOccupancyPercent=45   # 堆使用45%时开始并发标记
  -XX:+ParallelRefProcEnabled             # 并行引用处理
  -XX:MaxDirectMemorySize=1g              # 堆外内存上限1GB
  -XX:+HeapDumpOnOutOfMemoryError         # OOM时自动dump
  -XX:HeapDumpPath=/app/logs/heapdump.hprof
  -XX:+PrintGCDetails                     # JDK 8 GC日志(JDK 9+用-Xlog:gc)
  -XX:+PrintGCDateStamps
  -Xloggc:/app/logs/gc.log
  -XX:+UseGCLogFileRotation
  -XX:NumberOfGCLogFiles=5
  -XX:GCLogFileSize=20M
"
```

## 六、性能基准预估

### 6.1 单节点4C8G吞吐预估

| 消息大小 | 写入格式 | 压缩 | 预估吞吐(records/s) | 预估带宽(MB/s) |
|---------|---------|------|---------------------|---------------|
| 200B | Parquet | Snappy | 120,000+ | 24+ |
| 500B | Parquet | Snappy | 60,000-80,000 | 30-40 |
| 1KB | Parquet | Snappy | 35,000-50,000 | 35-50 |
| 500B | CSV | Gzip | 80,000-100,000 | 40-50 |
| 500B | CSV | None | 120,000+ | 60+ |

### 6.2 瓶颈对照

| 场景 | 瓶颈点 | 优化方向 |
|------|--------|---------|
| JSON消息大(>1KB) | JSON解析CPU | 增加Consumer线程数 |
| 字段多(>30个) | Parquet编码CPU | 减少不必要字段、使用CSV格式 |
| Topic多(>20个) | Buffer内存 | 减小flush阈值、增加节点 |
| Partition多(>20个) | 线程切换开销 | 合理设置Consumer线程数 |
| HDFS慢(高延迟) | flush等待 | 增加写入线程数、双Buffer |

## 七、性能测试方案

### 7.1 测试环境

```
Kafka: 3 Broker, 每台8C16G
HDFS: 3 DataNode, 每台8C16G, 3副本
Writer: 1节点, 4C8G

Topic: perf_test, 12 partitions
消息: JSON格式, 10个字段, 平均500B
```

### 7.2 测试场景

| 场景 | 配置 | 预期 |
|------|------|------|
| 基准吞吐 | 1 topic, Parquet, Snappy | >= 60K/s |
| 多Topic | 5 topics, Parquet, Snappy | >= 50K/s (总计) |
| CSV格式 | 1 topic, CSV, Gzip | >= 80K/s |
| 扩容测试 | 2节点, 12 partitions | >= 100K/s (总计) |
| Schema变更 | 运行中变更Schema | 变更期间不丢数据，<30s恢复 |
| 宕机恢复 | kill -9 后重启 | <30s恢复消费，数据不丢 |
