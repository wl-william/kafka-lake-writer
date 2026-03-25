<template>
  <div>
    <el-row :gutter="20" style="margin-bottom: 20px;">
      <el-col :span="6" v-for="card in statCards" :key="card.label">
        <el-card shadow="never">
          <div style="display: flex; align-items: center; justify-content: space-between;">
            <div>
              <div style="font-size: 13px; color: #909399;">{{ card.label }}</div>
              <div style="font-size: 28px; font-weight: 700; color: #303133; margin-top: 6px;">{{ card.value }}</div>
            </div>
            <i :class="card.icon" style="font-size: 40px; opacity: 0.15;"></i>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never">
      <div slot="header">Worker 节点状态</div>
      <el-table :data="nodes" stripe border style="width: 100%;">
        <el-table-column prop="nodeId" label="节点 ID" min-width="160" />
        <el-table-column label="状态" width="90">
          <template slot-scope="{row}">
            <el-tag :type="row.online ? 'success' : 'danger'" size="small">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="recordsPerSec" label="RPS" width="100" />
        <el-table-column label="Buffer" width="120">
          <template slot-scope="{row}">
            {{ row.bufferUsageMb }} MB
          </template>
        </el-table-column>
        <el-table-column label="运行时长" width="120">
          <template slot-scope="{row}">
            {{ formatUptime(row.uptimeSec) }}
          </template>
        </el-table-column>
        <el-table-column label="上次心跳" min-width="160">
          <template slot-scope="{row}">
            {{ row.lastHeartbeat | datetime }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script>
import { getStatusSummary } from '../api/status'

export default {
  name: 'Dashboard',
  filters: {
    datetime(val) {
      if (!val) return '-'
      return new Date(val).toLocaleString('zh-CN')
    }
  },
  data() {
    return {
      summary: {},
      nodes: [],
      timer: null
    }
  },
  computed: {
    statCards() {
      return [
        { label: '节点总数', value: this.summary.totalNodes || 0, icon: 'el-icon-cpu' },
        { label: '在线节点', value: this.summary.onlineNodes || 0, icon: 'el-icon-check' },
        { label: '总 RPS', value: (this.summary.totalRecordsPerSec || 0).toFixed(0), icon: 'el-icon-data-line' }
      ]
    }
  },
  created() {
    this.load()
    this.timer = setInterval(this.load, 15000)
  },
  beforeDestroy() {
    clearInterval(this.timer)
  },
  methods: {
    async load() {
      try {
        const res = await getStatusSummary()
        this.summary = res.data.data
        this.nodes = this.summary.nodes || []
      } catch (e) {
        // silent
      }
    },
    formatUptime(sec) {
      if (!sec) return '-'
      const h = Math.floor(sec / 3600)
      const m = Math.floor((sec % 3600) / 60)
      const s = sec % 60
      return `${h}h ${m}m ${s}s`
    }
  }
}
</script>
