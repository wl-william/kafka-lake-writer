<template>
  <div>
    <el-card shadow="never">
      <div slot="header" style="display: flex; align-items: center; justify-content: space-between;">
        <span>配置管理</span>
        <el-button type="primary" size="small" icon="el-icon-plus" @click="$router.push('/configs/new')">
          新增配置
        </el-button>
      </div>

      <el-form :inline="true" style="margin-bottom: 16px;">
        <el-form-item label="状态">
          <el-select v-model="filterStatus" clearable placeholder="全部" style="width: 120px;" @change="loadConfigs">
            <el-option label="ACTIVE" value="ACTIVE" />
            <el-option label="PAUSED" value="PAUSED" />
          </el-select>
        </el-form-item>
      </el-form>

      <el-table :data="configs" stripe border style="width: 100%;" v-loading="loading">
        <el-table-column prop="id" label="ID" width="60" />
        <el-table-column prop="topicName" label="Topic" min-width="160" />
        <el-table-column prop="sinkPath" label="目标路径" min-width="200" show-overflow-tooltip />
        <el-table-column prop="sinkFormat" label="格式" width="90" />
        <el-table-column prop="compression" label="压缩" width="90" />
        <el-table-column label="状态" width="90">
          <template slot-scope="{row}">
            <el-tag :type="row.status === 'ACTIVE' ? 'success' : 'warning'" size="small">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="version" label="版本" width="70" />
        <el-table-column label="操作" width="260" fixed="right">
          <template slot-scope="{row}">
            <el-button size="mini" @click="$router.push(`/configs/${row.id}/edit`)">编辑</el-button>
            <el-button size="mini" type="info" @click="$router.push(`/configs/${row.id}/monitor`)">监控</el-button>
            <el-button
              size="mini"
              :type="row.status === 'ACTIVE' ? 'warning' : 'success'"
              @click="togglePause(row)">
              {{ row.status === 'ACTIVE' ? '暂停' : '恢复' }}
            </el-button>
            <el-button size="mini" type="danger" @click="confirmDelete(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        style="margin-top: 16px; text-align: right;"
        :current-page="page + 1"
        :page-sizes="[10, 20, 50]"
        :page-size="size"
        :total="total"
        layout="total, sizes, prev, pager, next"
        @size-change="handleSizeChange"
        @current-change="handlePageChange" />
    </el-card>
  </div>
</template>

<script>
import { listConfigs, deleteConfig, pauseConfig, resumeConfig } from '../api/config'

export default {
  name: 'ConfigList',
  data() {
    return {
      configs: [],
      loading: false,
      filterStatus: '',
      page: 0,
      size: 20,
      total: 0
    }
  },
  created() {
    this.loadConfigs()
  },
  methods: {
    async loadConfigs() {
      this.loading = true
      try {
        const params = { page: this.page, size: this.size }
        if (this.filterStatus) params.status = this.filterStatus
        const res = await listConfigs(params)
        const p = res.data.data
        this.configs = p.content
        this.total = p.totalElements
      } finally {
        this.loading = false
      }
    },
    async togglePause(row) {
      try {
        if (row.status === 'ACTIVE') {
          await pauseConfig(row.id)
          this.$message.success('已暂停')
        } else {
          await resumeConfig(row.id)
          this.$message.success('已恢复')
        }
        this.loadConfigs()
      } catch (e) {
        this.$message.error('操作失败')
      }
    },
    confirmDelete(row) {
      this.$confirm(`确认删除 Topic: ${row.topicName}?`, '警告', {
        type: 'warning'
      }).then(async () => {
        await deleteConfig(row.id)
        this.$message.success('已删除')
        this.loadConfigs()
      }).catch(() => {})
    },
    handleSizeChange(val) {
      this.size = val
      this.page = 0
      this.loadConfigs()
    },
    handlePageChange(val) {
      this.page = val - 1
      this.loadConfigs()
    }
  }
}
</script>
