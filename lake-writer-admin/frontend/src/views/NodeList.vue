<template>
  <div>
    <el-card shadow="never">
      <div slot="header" style="display: flex; align-items: center; justify-content: space-between;">
        <span>Worker 节点状态</span>
        <el-button size="small" icon="el-icon-refresh" @click="load">刷新</el-button>
      </div>

      <el-table :data="nodes" stripe border style="width: 100%;" v-loading="loading">
        <el-table-column prop="nodeId" label="节点 ID" min-width="180" />
        <el-table-column label="状态" width="90">
          <template slot-scope="{row}">
            <el-tag :type="row.online ? 'success' : 'danger'" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="recordsPerSec" label="RPS" width="100">
          <template slot-scope="{row}">
            {{ (row.recordsPerSec || 0).toFixed(1) }}
          </template>
        </el-table-column>
        <el-table-column label="Buffer 用量" width="130">
          <template slot-scope="{row}">
            {{ row.bufferUsageMb }} MB
          </template>
        </el-table-column>
        <el-table-column label="运行时长" width="130">
          <template slot-scope="{row}">
            {{ formatUptime(row.uptimeSec) }}
          </template>
        </el-table-column>
        <el-table-column label="分配分区数" width="110">
          <template slot-scope="{row}">
            {{ countPartitions(row.assignedPartitions) }}
          </template>
        </el-table-column>
        <el-table-column label="上次心跳" min-width="160">
          <template slot-scope="{row}">
            {{ row.lastHeartbeat ? new Date(row.lastHeartbeat).toLocaleString('zh-CN') : '-' }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script>
import { getStatusNodes } from '../api/status'

export default {
  name: 'NodeList',
  data() {
    return {
      nodes: [],
      loading: false,
      timer: null
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
      this.loading = true
      try {
        const res = await getStatusNodes()
        this.nodes = res.data.data
      } finally {
        this.loading = false
      }
    },
    formatUptime(sec) {
      if (!sec) return '-'
      const h = Math.floor(sec / 3600)
      const m = Math.floor((sec % 3600) / 60)
      const s = sec % 60
      return `${h}h ${m}m ${s}s`
    },
    countPartitions(parts) {
      if (!parts) return 0
      return Object.values(parts).reduce((sum, arr) => sum + (arr ? arr.length : 0), 0)
    }
  }
}
</script>
