<template>
  <div>
    <el-card shadow="never">
      <div slot="header">变更日志</div>

      <el-form :inline="true" style="margin-bottom: 16px;">
        <el-form-item label="Config ID">
          <el-input v-model="searchId" placeholder="输入 Config ID" style="width: 160px;" clearable />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="load">查询</el-button>
        </el-form-item>
      </el-form>

      <el-table :data="logs" stripe border style="width: 100%;" v-loading="loading">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="configId" label="Config ID" width="90" />
        <el-table-column prop="topicName" label="Topic" min-width="140" />
        <el-table-column label="变更类型" width="100">
          <template slot-scope="{row}">
            <el-tag :type="changeTypeTag(row.changeType)" size="small">{{ row.changeType }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="operator" label="操作人" width="90" />
        <el-table-column label="变更时间" min-width="160">
          <template slot-scope="{row}">
            {{ row.createdAt ? new Date(row.createdAt).toLocaleString('zh-CN') : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="80">
          <template slot-scope="{row}">
            <el-button size="mini" @click="showDiff(row)">对比</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog title="配置变更对比" :visible.sync="diffVisible" width="70%">
      <el-row :gutter="20">
        <el-col :span="12">
          <div style="font-weight: 600; margin-bottom: 8px;">变更前</div>
          <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px; overflow: auto; max-height: 400px; font-size: 12px;">{{ formatJson(selected.beforeJson) }}</pre>
        </el-col>
        <el-col :span="12">
          <div style="font-weight: 600; margin-bottom: 8px;">变更后</div>
          <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px; overflow: auto; max-height: 400px; font-size: 12px;">{{ formatJson(selected.afterJson) }}</pre>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'ChangeLog',
  data() {
    return {
      logs: [],
      loading: false,
      searchId: '',
      diffVisible: false,
      selected: {}
    }
  },
  created() {
    this.load()
  },
  methods: {
    async load() {
      if (!this.searchId) {
        this.logs = []
        return
      }
      this.loading = true
      try {
        const res = await axios.get(`/api/v1/changelog/config/${this.searchId}`)
        this.logs = res.data.data
      } finally {
        this.loading = false
      }
    },
    changeTypeTag(type) {
      const map = { CREATED: 'success', UPDATED: '', DELETED: 'danger', PAUSED: 'warning', RESUMED: 'success' }
      return map[type] || 'info'
    },
    showDiff(row) {
      this.selected = row
      this.diffVisible = true
    },
    formatJson(str) {
      if (!str) return '(无)'
      try {
        return JSON.stringify(JSON.parse(str), null, 2)
      } catch (e) {
        return str
      }
    }
  }
}
</script>
