<template>
  <div>
    <el-page-header @back="$router.push('/configs')" :content="`监控: ${config.topicName || ''}`" style="margin-bottom: 20px;" />

    <el-row :gutter="20" style="margin-bottom: 20px;">
      <el-col :span="6">
        <el-card shadow="never">
          <div class="stat-label">版本</div>
          <div class="stat-value">{{ config.version }}</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never">
          <div class="stat-label">状态</div>
          <div class="stat-value">
            <el-tag :type="config.status === 'ACTIVE' ? 'success' : 'warning'" size="small">
              {{ config.status }}
            </el-tag>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never">
          <div class="stat-label">格式</div>
          <div class="stat-value">{{ config.sinkFormat }}</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never">
          <div class="stat-label">压缩</div>
          <div class="stat-value">{{ config.compression }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20">
      <el-col :span="14">
        <el-card shadow="never" style="margin-bottom: 20px;">
          <div slot="header">消费延迟趋势</div>
          <consumer-lag-chart :topic="config.topicName" />
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card shadow="never">
          <div slot="header">消费该 Topic 的节点</div>
          <el-table :data="relatedNodes" border size="small">
            <el-table-column prop="nodeId" label="节点" min-width="120" />
            <el-table-column label="状态" width="80">
              <template slot-scope="{row}">
                <el-tag :type="row.online ? 'success' : 'danger'" size="mini">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="recordsPerSec" label="RPS" width="70" />
          </el-table>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" style="margin-top: 20px;">
      <div slot="header">目标路径</div>
      <code style="font-size: 13px; color: #409EFF;">{{ config.sinkPath }}</code>
    </el-card>
  </div>
</template>

<script>
import { getConfig } from '../api/config'
import { getStatusNodes } from '../api/status'
import ConsumerLagChart from '../components/ConsumerLagChart.vue'

export default {
  name: 'MonitorDetail',
  components: { ConsumerLagChart },
  data() {
    return {
      config: {},
      allNodes: []
    }
  },
  computed: {
    relatedNodes() {
      if (!this.config.topicName) return []
      return this.allNodes.filter(n => {
        const parts = n.assignedPartitions
        if (!parts) return false
        return Object.keys(parts).some(t => t === this.config.topicName)
      })
    }
  },
  async created() {
    const [cRes, nRes] = await Promise.all([
      getConfig(this.$route.params.id),
      getStatusNodes()
    ])
    this.config = cRes.data.data
    this.allNodes = nRes.data.data
  }
}
</script>

<style scoped>
.stat-label { font-size: 13px; color: #909399; margin-bottom: 6px; }
.stat-value { font-size: 22px; font-weight: 700; }
</style>
