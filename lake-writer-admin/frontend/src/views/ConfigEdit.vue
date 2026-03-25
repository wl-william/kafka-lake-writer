<template>
  <div>
    <el-card shadow="never">
      <div slot="header">{{ isEdit ? '编辑配置' : '新增配置' }}</div>

      <el-form ref="form" :model="form" :rules="rules" label-width="130px" style="max-width: 900px;">
        <el-form-item label="Topic 名称" prop="topicName">
          <el-input v-model="form.topicName" :disabled="isEdit" placeholder="kafka.topic.name 或正则表达式" />
        </el-form-item>

        <el-form-item label="目标路径" prop="sinkPath">
          <el-input v-model="form.sinkPath" placeholder="/data/{topic}/{dt}/{hour}/" @input="onPathChange" />
        </el-form-item>

        <el-form-item label="路径预览">
          <path-preview :template="form.sinkPath" :topic="form.topicName || 'sample_topic'" />
        </el-form-item>

        <el-form-item label="文件格式" prop="sinkFormat">
          <el-select v-model="form.sinkFormat">
            <el-option label="PARQUET" value="PARQUET" />
            <el-option label="CSV" value="CSV" />
          </el-select>
        </el-form-item>

        <el-form-item label="压缩格式" prop="compression">
          <el-select v-model="form.compression">
            <el-option label="SNAPPY" value="SNAPPY" />
            <el-option label="GZIP" value="GZIP" />
            <el-option label="NONE" value="NONE" />
          </el-select>
        </el-form-item>

        <el-form-item label="刷盘行数" prop="flushRows">
          <el-input-number v-model="form.flushRows" :min="100" :max="10000000" />
        </el-form-item>

        <el-form-item label="刷盘间隔(秒)" prop="flushIntervalSec">
          <el-input-number v-model="form.flushIntervalSec" :min="10" :max="3600" />
        </el-form-item>

        <el-form-item label="Schema 定义" prop="schemaJson">
          <schema-editor v-model="form.schemaJson" />
        </el-form-item>

        <el-form-item label="描述">
          <el-input v-model="form.description" type="textarea" :rows="2" />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" :loading="saving" @click="save">保存</el-button>
          <el-button @click="$router.push('/configs')">取消</el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script>
import { getConfig, createConfig, updateConfig, validateConfig } from '../api/config'
import SchemaEditor from '../components/SchemaEditor.vue'
import PathPreview from '../components/PathPreview.vue'

export default {
  name: 'ConfigEdit',
  components: { SchemaEditor, PathPreview },
  data() {
    return {
      form: {
        topicName: '',
        sinkPath: '',
        sinkFormat: 'PARQUET',
        compression: 'SNAPPY',
        flushRows: 100000,
        flushBytes: 134217728,
        flushIntervalSec: 300,
        partitionBy: 'dt',
        schemaJson: '{"fields":[]}',
        description: ''
      },
      saving: false,
      rules: {
        topicName: [{ required: true, message: '请输入 Topic 名称', trigger: 'blur' }],
        sinkPath: [{ required: true, message: '请输入目标路径', trigger: 'blur' }],
        sinkFormat: [{ required: true, trigger: 'change' }],
        schemaJson: [{ required: true, message: '请定义 Schema', trigger: 'change' }]
      }
    }
  },
  computed: {
    isEdit() {
      return !!this.$route.params.id
    }
  },
  async created() {
    if (this.isEdit) {
      const res = await getConfig(this.$route.params.id)
      this.form = Object.assign({}, this.form, res.data.data)
    }
  },
  methods: {
    onPathChange() {
      // PathPreview re-renders reactively
    },
    async save() {
      await this.$refs.form.validate()
      const vRes = await validateConfig({
        schemaJson: this.form.schemaJson,
        sinkPath: this.form.sinkPath
      })
      if (!vRes.data.data.valid) {
        this.$message.error('校验失败: ' + vRes.data.data.errors.join('; '))
        return
      }
      this.saving = true
      try {
        if (this.isEdit) {
          await updateConfig(this.$route.params.id, this.form)
          this.$message.success('更新成功，将在30秒内生效')
        } else {
          await createConfig(this.form)
          this.$message.success('创建成功，将在30秒内生效')
        }
        this.$router.push('/configs')
      } catch (e) {
        this.$message.error('保存失败')
      } finally {
        this.saving = false
      }
    }
  }
}
</script>
