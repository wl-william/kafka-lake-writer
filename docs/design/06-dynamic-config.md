# Kafka-Lake-Writer 动态配置设计

## 一、设计目标

实现Topic配置的**运行时变更**，包括：
- 新增/移除/暂停Topic → 无需重启
- 变更Topic Schema → 自动flush旧数据，切换新Schema
- 变更写入路径/格式/Flush策略 → 30秒内生效

## 二、配置变更检测

### 2.1 轮询机制

```java
@Component
public class DynamicConfigManager {

    private volatile Map<String, TopicSinkConfig> currentConfigMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();

    @PostConstruct
    public void start() {
        // 每30秒拉取一次最新配置
        poller.scheduleAtFixedRate(this::syncConfig, 0, 30, TimeUnit.SECONDS);
    }

    private void syncConfig() {
        try {
            // 从MySQL拉取所有ACTIVE配置
            Map<String, TopicSinkConfig> latestMap = configRepository.findAllActive()
                .stream()
                .collect(Collectors.toMap(TopicSinkConfig::getTopicName, Function.identity()));

            // 计算diff
            ConfigDiff diff = computeDiff(currentConfigMap, latestMap);

            if (diff.isEmpty()) return;

            log.info("[ConfigSync] 检测到变更: added={}, removed={}, schemaChanged={}, otherChanged={}",
                diff.getAdded().size(), diff.getRemoved().size(),
                diff.getSchemaChanged().size(), diff.getOtherChanged().size());

            // 处理各类变更
            handleTopicAdded(diff.getAdded());
            handleTopicRemoved(diff.getRemoved());
            handleSchemaChanged(diff.getSchemaChanged());
            handleOtherChanged(diff.getOtherChanged());

            // 更新本地配置缓存
            currentConfigMap = new ConcurrentHashMap<>(latestMap);

        } catch (Exception e) {
            log.error("[ConfigSync] 配置同步失败, 保持当前配置", e);
            metrics.counter("config_sync_failure_total").increment();
        }
    }
}
```

### 2.2 配置Diff算法

```java
public class ConfigDiff {

    private final List<TopicSinkConfig> added;          // 新增的topic
    private final List<TopicSinkConfig> removed;        // 移除的topic
    private final List<TopicSinkConfig> schemaChanged;  // schema变更的topic
    private final List<TopicSinkConfig> otherChanged;   // 其他配置变更(路径/格式/flush策略)

    public static ConfigDiff compute(Map<String, TopicSinkConfig> oldMap,
                                     Map<String, TopicSinkConfig> newMap) {
        List<TopicSinkConfig> added = new ArrayList<>();
        List<TopicSinkConfig> removed = new ArrayList<>();
        List<TopicSinkConfig> schemaChanged = new ArrayList<>();
        List<TopicSinkConfig> otherChanged = new ArrayList<>();

        // 新增: newMap中有但oldMap没有
        for (Map.Entry<String, TopicSinkConfig> entry : newMap.entrySet()) {
            if (!oldMap.containsKey(entry.getKey())) {
                added.add(entry.getValue());
            }
        }

        // 移除: oldMap中有但newMap没有
        for (Map.Entry<String, TopicSinkConfig> entry : oldMap.entrySet()) {
            if (!newMap.containsKey(entry.getKey())) {
                removed.add(entry.getValue());
            }
        }

        // 变更: 两边都有，但version不同
        for (Map.Entry<String, TopicSinkConfig> entry : newMap.entrySet()) {
            TopicSinkConfig oldConfig = oldMap.get(entry.getKey());
            TopicSinkConfig newConfig = entry.getValue();
            if (oldConfig != null && oldConfig.getVersion() != newConfig.getVersion()) {
                if (!oldConfig.getSchemaJson().equals(newConfig.getSchemaJson())) {
                    schemaChanged.add(newConfig);
                } else {
                    otherChanged.add(newConfig);
                }
            }
        }

        return new ConfigDiff(added, removed, schemaChanged, otherChanged);
    }
}
```

## 三、各类变更处理策略

### 3.1 新增Topic

```
管理员INSERT新topic配置
        │
        ▼
ConfigManager检测到新增
        │
        ▼
更新Consumer订阅列表: consumer.subscribe(newTopicSet, rebalanceListener)
        │
        ▼
Kafka触发Rebalance，分配新Topic的Partition
        │
        ▼
onPartitionsAssigned回调 → 为新分区创建WriteBuffer
        │
        ▼
开始消费新Topic数据
```

### 3.2 移除Topic

```
管理员DELETE或UPDATE status='PAUSED'
        │
        ▼
ConfigManager检测到移除
        │
        ▼
紧急flush该topic所有Buffer
        │
        ▼
提交offset
        │
        ▼
从Consumer订阅列表中移除
        │
        ▼
Kafka触发Rebalance，回收该Topic的Partition
```

### 3.3 Schema变更（最复杂）

```
管理员UPDATE schema_json字段
        │
        ▼
ConfigManager检测到schema变更
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  Step 1: 按旧Schema紧急flush当前Buffer  │
  │          保证已积累的数据用正确的Schema写入│
  └─────────────────────┬───────────────────┘
                        │
  ┌─────────────────────▼───────────────────┐
  │  Step 2: 提交当前offset                  │
  └─────────────────────┬───────────────────┘
                        │
  ┌─────────────────────▼───────────────────┐
  │  Step 3: 用新Schema重建WriteBuffer       │
  │          新数据将按新Schema解析和写入     │
  └─────────────────────┬───────────────────┘
                        │
  ┌─────────────────────▼───────────────────┐
  │  Step 4: 继续正常消费                    │
  │          后续数据按新Schema写入新文件     │
  └─────────────────────────────────────────┘
```

```java
private void handleSchemaChanged(List<TopicSinkConfig> configs) {
    for (TopicSinkConfig newConfig : configs) {
        String topic = newConfig.getTopicName();
        log.info("[SchemaChange] topic={}, 开始切换Schema", topic);

        // Step 1: 紧急flush旧Buffer
        List<WriteBuffer> buffers = bufferManager.getBuffersByTopic(topic);
        for (WriteBuffer buffer : buffers) {
            if (!buffer.isEmpty()) {
                FlushResult result = buffer.forceFlush();
                if (result.isSuccess()) {
                    // Step 2: 提交offset
                    consumerPool.commitSync(buffer.getTopicPartition(), buffer.getLastOffset());
                }
            }
        }

        // Step 3: 用新Schema重建Buffer
        MessageType newParquetSchema = schemaConverter.toParquetSchema(newConfig.getSchemaJson(), topic);
        for (WriteBuffer buffer : buffers) {
            bufferManager.recreateBuffer(buffer.getTopicPartition(), newConfig, newParquetSchema);
        }

        log.info("[SchemaChange] topic={}, Schema切换完成", topic);
    }
}
```

### 3.4 其他配置变更（路径/格式/Flush策略）

```java
private void handleOtherChanged(List<TopicSinkConfig> configs) {
    for (TopicSinkConfig newConfig : configs) {
        String topic = newConfig.getTopicName();

        // 路径或格式变更: 需要flush当前Buffer(旧路径/旧格式)，再用新配置
        if (isPathOrFormatChanged(currentConfigMap.get(topic), newConfig)) {
            List<WriteBuffer> buffers = bufferManager.getBuffersByTopic(topic);
            for (WriteBuffer buffer : buffers) {
                if (!buffer.isEmpty()) {
                    buffer.forceFlush();
                    consumerPool.commitSync(buffer.getTopicPartition(), buffer.getLastOffset());
                }
                bufferManager.recreateBuffer(buffer.getTopicPartition(), newConfig);
            }
        } else {
            // 仅flush策略变更: 直接更新Buffer的阈值参数，不需要flush
            bufferManager.updateFlushPolicy(topic, newConfig);
        }

        log.info("[ConfigChange] topic={}, 配置更新完成", topic);
    }
}
```

## 四、正则匹配Topic

### 4.1 Topic匹配引擎

```java
public class TopicMatcher {

    private final Map<String, TopicSinkConfig> exactConfigs = new HashMap<>();
    private final Map<Pattern, TopicSinkConfig> regexConfigs = new LinkedHashMap<>();

    public void reload(List<TopicSinkConfig> configs) {
        exactConfigs.clear();
        regexConfigs.clear();

        for (TopicSinkConfig config : configs) {
            if ("EXACT".equals(config.getMatchType())) {
                exactConfigs.put(config.getTopicName(), config);
            } else if ("REGEX".equals(config.getMatchType())) {
                regexConfigs.put(Pattern.compile(config.getTopicName()), config);
            }
        }
    }

    /**
     * 查找topic对应的配置
     * 优先精确匹配，其次正则匹配(按配置顺序, 第一个匹配的生效)
     */
    public TopicSinkConfig match(String topic) {
        // 精确匹配优先
        TopicSinkConfig config = exactConfigs.get(topic);
        if (config != null) return config;

        // 正则匹配
        for (Map.Entry<Pattern, TopicSinkConfig> entry : regexConfigs.entrySet()) {
            if (entry.getKey().matcher(topic).matches()) {
                return entry.getValue();
            }
        }

        return null;  // 无匹配配置
    }

    /**
     * 获取所有需要订阅的topic集合(用于Consumer.subscribe)
     *
     * 精确匹配的topic直接加入列表
     * 正则匹配需要从Kafka获取所有topic列表后过滤
     */
    public Set<String> resolveSubscriptionTopics(Set<String> allKafkaTopics) {
        Set<String> result = new HashSet<>(exactConfigs.keySet());

        for (Map.Entry<Pattern, TopicSinkConfig> entry : regexConfigs.entrySet()) {
            for (String topic : allKafkaTopics) {
                if (entry.getKey().matcher(topic).matches()) {
                    result.add(topic);
                }
            }
        }

        return result;
    }
}
```

## 五、REST管理接口

### 5.1 API设计

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/api/v1/configs` | 查询所有配置(支持分页/状态过滤) |
| GET | `/api/v1/configs/{id}` | 查询单个配置 |
| POST | `/api/v1/configs` | 新增配置 |
| PUT | `/api/v1/configs/{id}` | 更新配置(Topic/Schema/路径/Flush策略) |
| DELETE | `/api/v1/configs/{id}` | 删除配置 |
| PUT | `/api/v1/configs/{id}/pause` | 暂停topic |
| PUT | `/api/v1/configs/{id}/resume` | 恢复topic |
| GET | `/api/v1/configs/{id}/changelog` | 查询配置变更历史 |
| POST | `/api/v1/configs/validate` | 校验配置(Schema格式/路径模板合法性) |
| GET | `/api/v1/status` | 查询运行状态(各topic消费进度/Buffer状态) |
| GET | `/api/v1/status/{topic}` | 查询指定topic状态 |
| GET | `/api/v1/status/nodes` | 查询所有消费节点状态 |

### 5.2 请求/响应示例

**新增配置（日期路径示例）：**
```http
POST /api/v1/configs
Content-Type: application/json

{
  "topicName": "dev_stats",
  "matchType": "EXACT",
  "schemaJson": {
    "fields": [
      {"name": "device_id", "type": "LONG"},
      {"name": "metric_name", "type": "STRING"},
      {"name": "metric_value", "type": "DOUBLE"},
      {"name": "ts", "type": "LONG"}
    ]
  },
  "sinkFormat": "PARQUET",
  "sinkPath": "/hdfs/kafka/data/dev_stats/{date}",
  "compression": "SNAPPY",
  "flushRows": 500000,
  "flushIntervalSec": 600,
  "description": "设备统计数据，按天增量写入"
}
```

**响应：**
```json
{
  "code": 200,
  "data": {
    "id": 5,
    "topicName": "dev_stats",
    "sinkPath": "/hdfs/kafka/data/dev_stats/{date}",
    "resolvedPathPreview": "/hdfs/kafka/data/dev_stats/2026-03-22",
    "status": "ACTIVE",
    "version": 1,
    "createdAt": "2026-03-22T14:30:00"
  },
  "message": "配置创建成功，将在30秒内生效"
}
```

**查询运行状态：**
```http
GET /api/v1/status
```

```json
{
  "code": 200,
  "data": {
    "nodeId": "pod-01",
    "uptime": "2d 5h 30m",
    "topics": [
      {
        "topic": "dev_stats",
        "sinkPath": "/hdfs/kafka/data/dev_stats/{date}",
        "currentResolvedPath": "/hdfs/kafka/data/dev_stats/2026-03-22",
        "assignedPartitions": [0, 1, 2],
        "totalConsumed": 58320000,
        "totalFlushed": 58200000,
        "bufferStatus": {
          "P0": {"rows": 45000, "bytes": 22500000, "ageSeconds": 120},
          "P1": {"rows": 38000, "bytes": 19000000, "ageSeconds": 95},
          "P2": {"rows": 52000, "bytes": 26000000, "ageSeconds": 150}
        },
        "consumerLag": {
          "P0": 1200,
          "P1": 800,
          "P2": 2100
        }
      }
    ]
  }
}
```

## 六、Web可视化管理界面（独立Admin服务）

> **核心原则：管理与消费分离。** Web管理界面运行在独立的 `lake-writer-admin` 服务中，
> 不耦合到任何消费Worker节点。Worker节点只做消费写入 + 心跳上报，保持轻量纯粹。

### 6.1 技术方案

- **Admin服务**：Spring Boot 2.7 + Vue.js 2.x + Element UI，独立部署1个实例
- **Worker节点**：纯Java消费进程，无Web/无管理API，按需部署N个实例
- **数据交互**：Admin和Worker通过MySQL间接通信，不直接调用

```
┌───────────────────────────────────────────────────────────────────┐
│                         数据流向                                   │
│                                                                   │
│  Admin服务                  MySQL                 Worker节点(N个)  │
│  ┌──────────┐          ┌──────────┐          ┌──────────┐        │
│  │ 写入配置  │────────►│ config表 │◄─────────│ 读取配置  │        │
│  │ 读取状态  │◄────────│ heartbeat│◄─────────│ 写入心跳  │        │
│  │ 读取审计  │◄────────│ changelog│          │          │        │
│  └──────────┘          └──────────┘          └──────────┘        │
│                                                                   │
│  Admin不直接与Worker通信, Worker不暴露任何业务API                  │
└───────────────────────────────────────────────────────────────────┘
```

```
访问Admin: http://{admin-host}:8080/

┌─────────────────────────────────────────────────────────────────┐
│  Kafka-Lake-Writer 管理控制台                                    │
├─────────┬───────────────────────────────────────────────────────┤
│         │                                                       │
│  导航栏  │   主内容区                                            │
│         │                                                       │
│ ○ 仪表盘 │   根据左侧导航切换不同页面                            │
│ ○ 配置管理│                                                       │
│ ○ 消费监控│                                                       │
│ ○ 变更历史│                                                       │
│ ○ 节点状态│   (从worker_heartbeat表聚合展示所有Worker状态)        │
│         │                                                       │
└─────────┴───────────────────────────────────────────────────────┘
```

### 6.2 页面功能

#### 页面1: 仪表盘 (Dashboard)

全局概览，一页掌握系统状态：

```
┌──────────────────────────────────────────────────────────────┐
│  仪表盘                                                       │
│                                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │ Topic总数 │ │ 活跃Topic │ │ 消费总速率 │ │ 在线节点  │       │
│  │    12     │ │    10    │ │ 85,200/s │ │   2/2    │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  各Topic消费速率 (实时折线图, 最近30分钟)              │   │
│  │  ─── orders: 32,000/s                                │   │
│  │  ─── user_events: 28,000/s                           │   │
│  │  ─── dev_stats: 25,200/s                             │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  消费延迟Top5 (表格)                                  │   │
│  │  Topic        │ MaxLag  │ 状态  │ 操作              │   │
│  │  orders       │ 1,200   │ ✅正常 │ [查看详情]         │   │
│  │  user_events  │ 15,800  │ ⚠️偏高 │ [查看详情]         │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

#### 页面2: 配置管理 (ConfigList / ConfigEdit)

Topic配置的增删改查，核心管理页面：

```
┌──────────────────────────────────────────────────────────────┐
│  配置管理                                    [+ 新增配置]     │
│                                                              │
│  搜索: [_________]  状态: [全部 ▼]  格式: [全部 ▼]  [搜索]  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Topic        │ 匹配  │ 格式    │ 路径(今日)          │ 状态  │ 操作            │
│  ├──────────────┼──────┼────────┼───────────────────────┼──────┼────────────────┤
│  │ dev_stats    │ 精确  │ Parquet│ /hdfs/.../2026-03-22 │ ✅活跃│ [编辑][暂停][删除]│
│  │ orders       │ 精确  │ Parquet│ /hdfs/.../2026-03-22 │ ✅活跃│ [编辑][暂停][删除]│
│  │ user_event_.*│ 正则  │ Parquet│ /hdfs/.../2026-03-22 │ ✅活跃│ [编辑][暂停][删除]│
│  │ sys_monitor  │ 精确  │ CSV    │ /data/.../2026-03-22 │ ⏸暂停│ [编辑][恢复][删除]│
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**新增/编辑配置弹窗：**

```
┌─────────────────────────────────────────────────┐
│  新增Topic配置                              [×] │
│                                                 │
│  Topic名称:  [dev_stats________]                │
│  匹配方式:   ○ 精确  ○ 正则                     │
│                                                 │
│  ── 写入配置 ──────────────────────────────     │
│  写入格式:   [Parquet ▼]                        │
│  压缩算法:   [Snappy  ▼]                        │
│  存储路径:   [/hdfs/kafka/data/dev_stats/{date}]│
│  路径预览:   /hdfs/kafka/data/dev_stats/2026-03-22│
│             ↑ 实时预览,输入路径模板后自动解析      │
│                                                 │
│  ── Schema定义 ────────────────────────────     │
│  ┌─────────┬─────────┬──────────┬────────┐     │
│  │ 字段名   │ 类型     │ 可为空   │ 操作    │     │
│  ├─────────┼─────────┼──────────┼────────┤     │
│  │ device_id│ LONG    │ □        │ [×][↕] │     │
│  │ metric   │ STRING  │ ☑        │ [×][↕] │     │
│  │ value    │ DOUBLE  │ ☑        │ [×][↕] │     │
│  │ ts       │ LONG    │ □        │ [×][↕] │     │
│  └─────────┴─────────┴──────────┴────────┘     │
│  [+ 添加字段]                                   │
│                                                 │
│  ── Flush策略 ─────────────────────────────     │
│  行数阈值:     [500000__] 条                     │
│  大小阈值:     [256_____] MB                     │
│  时间间隔:     [600_____] 秒                     │
│                                                 │
│  描述:  [设备统计数据,按天增量写入___________]    │
│                                                 │
│         [取消]                    [保存配置]      │
│  ⓘ 保存后将在30秒内自动生效,无需重启消费实例     │
└─────────────────────────────────────────────────┘
```

#### 页面3: 消费监控 (MonitorDetail)

每个Topic的详细消费状况：

```
┌──────────────────────────────────────────────────────────────┐
│  消费监控 > dev_stats                                        │
│                                                              │
│  当前路径: /hdfs/kafka/data/dev_stats/2026-03-22             │
│  消费节点: pod-01(P0,P1,P2)  pod-02(P3,P4,P5)              │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  各分区消费进度                                        │   │
│  │  Partition │ 当前Offset │ 最新Offset │  Lag   │ 节点  │   │
│  │  P0        │ 1,582,390  │ 1,583,590  │ 1,200  │ pod-01│   │
│  │  P1        │ 1,490,200  │ 1,491,000  │   800  │ pod-01│   │
│  │  P2        │ 1,612,300  │ 1,614,400  │ 2,100  │ pod-01│   │
│  │  P3        │ 1,520,100  │ 1,520,900  │   800  │ pod-02│   │
│  │  P4        │ 1,498,000  │ 1,498,500  │   500  │ pod-02│   │
│  │  P5        │ 1,555,200  │ 1,556,000  │   800  │ pod-02│   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Buffer状态                                           │   │
│  │  Partition │ 行数    │ 大小       │ 已持续    │ 进度  │   │
│  │  P0        │ 45,000  │ 22.5 MB    │ 2min     │ ██░ 9%│   │
│  │  P1        │ 38,000  │ 19.0 MB    │ 1.5min   │ █░░ 8%│   │
│  │  ...       │         │            │          │       │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  消费速率趋势 (ECharts折线图, 最近1小时)               │   │
│  │  ▄▅▆█▇▆▅▆▇█▇▆  avg: 32,000/s                       │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

#### 页面4: 变更历史 (ChangeLog)

配置变更审计追踪：

```
┌──────────────────────────────────────────────────────────────┐
│  变更历史                                                     │
│                                                              │
│  Topic: [全部 ▼]  类型: [全部 ▼]  时间: [最近7天 ▼]  [搜索] │
│                                                              │
│  时间              │ Topic      │ 类型      │ 操作人  │ 详情  │
│  2026-03-22 14:30  │ dev_stats  │ 新增配置  │ admin  │ [查看]│
│  2026-03-22 10:15  │ orders     │ Schema变更│ admin  │ [查看]│
│  2026-03-21 16:00  │ sys_monitor│ 暂停      │ admin  │ [查看]│
│  2026-03-21 09:30  │ user_event │ 路径变更  │ ops    │ [查看]│
│                                                              │
│  [查看] → 弹窗展示变更前后的JSON对比(diff视图)               │
└──────────────────────────────────────────────────────────────┘
```

### 6.3 Admin与Worker交互流程（通过MySQL间接通信）

```
┌──────────────────┐       ┌────────────┐       ┌──────────────────┐
│  Admin管理服务     │       │   MySQL    │       │  Worker消费节点    │
│  (独立部署)       │       │            │       │  (独立部署, N个)   │
│                  │       │            │       │                  │
│  用户在Web界面    │ INSERT │            │       │                  │
│  新增Topic配置 ──┼──────►│ config表   │       │                  │
│                  │       │ version+1  │       │                  │
│                  │       │            │ SELECT│                  │
│                  │       │            │◄──────┤ ConfigPoller     │
│                  │       │            │       │ (每30秒只读轮询)  │
│                  │       │            │       │     │            │
│                  │       │            │       │     ▼ diff检测    │
│                  │       │            │       │ 发现新Topic       │
│                  │       │            │       │     │            │
│                  │       │            │       │     ▼            │
│                  │       │            │       │ 更新Consumer订阅  │
│                  │       │            │       │ 开始消费新Topic   │
│                  │       │            │       │     │            │
│                  │       │            │       │     ▼            │
│                  │       │            │ INSERT│ HeartbeatReporter│
│  用户在Web界面    │ SELECT│ heartbeat表│◄──────┤ (每15秒上报状态)  │
│  查看消费状态  ──┼──────►│            │       │                  │
│                  │       │            │       │                  │
│  显示各Worker    │       │            │       │                  │
│  各分区进度/Lag  │       │            │       │                  │
└──────────────────┘       └────────────┘       └──────────────────┘

关键: Admin和Worker之间零直接通信, 全部通过MySQL中转
```

### 6.4 Worker心跳上报机制

Worker节点每15秒向MySQL `worker_heartbeat` 表写入当前状态：

```java
@Component
public class HeartbeatReporter {

    private final WorkerHeartbeatRepository heartbeatRepo;
    private final WriteBufferManager bufferManager;
    private final KafkaConsumerPool consumerPool;
    private final NodeIdentity nodeIdentity;

    @Scheduled(fixedDelay = 15000)  // 每15秒
    public void report() {
        WorkerHeartbeat hb = new WorkerHeartbeat();
        hb.setNodeId(nodeIdentity.getNodeId());
        hb.setStatus("ONLINE");
        hb.setAssignedPartitions(JSON.toJSONString(consumerPool.getAssignedPartitions()));
        hb.setConsumerStatus(JSON.toJSONString(consumerPool.getPartitionStatus()));
        hb.setRecordsPerSec(metrics.getRecentRecordsPerSec());
        hb.setBufferUsageBytes(bufferManager.getTotalBufferBytes());
        hb.setUptimeSec(getUptimeSeconds());
        hb.setLastHeartbeat(new Date());

        // INSERT ON DUPLICATE KEY UPDATE (按node_id upsert)
        heartbeatRepo.upsert(hb);
    }
}
```

Admin服务的 `StatusAggregator` 从此表读取所有Worker状态进行聚合展示：

```java
@Service
public class StatusAggregator {

    /**
     * 聚合所有Worker节点状态
     * - 心跳超过60秒未更新的节点标记为OFFLINE
     */
    public List<WorkerStatus> aggregateAll() {
        List<WorkerHeartbeat> heartbeats = heartbeatRepo.findAll();
        Date threshold = new Date(System.currentTimeMillis() - 60_000);

        return heartbeats.stream().map(hb -> {
            WorkerStatus ws = new WorkerStatus();
            ws.setNodeId(hb.getNodeId());
            ws.setOnline(hb.getLastHeartbeat().after(threshold));
            ws.setPartitions(JSON.parseObject(hb.getAssignedPartitions()));
            ws.setRecordsPerSec(hb.getRecordsPerSec());
            ws.setBufferUsageBytes(hb.getBufferUsageBytes());
            ws.setLastHeartbeat(hb.getLastHeartbeat());
            return ws;
        }).collect(Collectors.toList());
    }
}
```

### 6.5 消费实例自动感知配置变更

配置在Web界面修改后，所有Worker节点通过MySQL轮询自动感知：

```
Admin Web界面操作 → MySQL config表更新(version+1, updated_at刷新)
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   Worker-1          Worker-2         Worker-3
   ConfigPoller      ConfigPoller     ConfigPoller
   (每30秒只读)      (每30秒只读)     (每30秒只读)
        │                │                │
        ▼                ▼                ▼
   SELECT * FROM topic_sink_config WHERE updated_at > lastSyncTime
        │                │                │
        ▼                ▼                ▼
   发现version变化 → 触发ConfigDiff → 按变更类型处理
   (新增/删除/Schema变更/路径变更)
```

**保证：**
- 所有Worker节点最迟30秒内感知变更
- Schema变更时自动flush旧数据 → 用新Schema接收新数据
- 路径变更时自动切换到新路径
- 整个过程无需重启任何Worker实例
- Admin服务重启/宕机不影响Worker消费（Worker只依赖MySQL）
