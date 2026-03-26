<template>
  <div>
    <el-table :data="fields" border size="small" style="margin-bottom: 10px;">
      <el-table-column label="字段名" min-width="140">
        <template slot-scope="{$index}">
          <el-input v-model="fields[$index].name" size="mini" placeholder="field_name" @input="emit" />
        </template>
      </el-table-column>
      <el-table-column label="类型" width="120">
        <template slot-scope="{$index}">
          <el-select v-model="fields[$index].type" size="mini" @change="emit">
            <el-option v-for="t in typeOptions" :key="t" :label="t" :value="t" />
          </el-select>
        </template>
      </el-table-column>
      <el-table-column label="可空" width="70">
        <template slot-scope="{$index}">
          <el-checkbox v-model="fields[$index].nullable" @change="emit" />
        </template>
      </el-table-column>
      <el-table-column label="操作" width="120">
        <template slot-scope="{$index}">
          <el-button size="mini" :disabled="$index === 0" icon="el-icon-top" circle @click="moveUp($index)" />
          <el-button size="mini" :disabled="$index === fields.length - 1" icon="el-icon-bottom" circle @click="moveDown($index)" />
          <el-button size="mini" type="danger" icon="el-icon-delete" circle @click="remove($index)" />
        </template>
      </el-table-column>
    </el-table>
    <el-button size="small" icon="el-icon-plus" @click="addField">添加字段</el-button>
  </div>
</template>

<script>
export default {
  name: 'SchemaEditor',
  model: { prop: 'value', event: 'input' },
  props: {
    value: { type: String, default: '[]' }
  },
  data() {
    return {
      fields: [],
      typeOptions: ['STRING', 'INT', 'LONG', 'FLOAT', 'DOUBLE', 'BOOLEAN']
    }
  },
  watch: {
    value: {
      immediate: true,
      handler(v) {
        try {
          const parsed = JSON.parse(v)
          // Backend format: {"fields":[...]}
          // Also accept plain array for backwards compatibility
          const arr = Array.isArray(parsed) ? parsed : (parsed.fields || [])
          this.fields = arr.map(f => ({
            name: f.name || '',
            type: f.type || 'STRING',
            nullable: f.nullable !== false
          }))
        } catch (e) {
          this.fields = []
        }
      }
    }
  },
  methods: {
    addField() {
      this.fields.push({ name: '', type: 'STRING', nullable: true })
      this.emit()
    },
    remove(index) {
      this.fields.splice(index, 1)
      this.emit()
    },
    moveUp(index) {
      const tmp = this.fields[index - 1]
      this.$set(this.fields, index - 1, this.fields[index])
      this.$set(this.fields, index, tmp)
      this.emit()
    },
    moveDown(index) {
      const tmp = this.fields[index + 1]
      this.$set(this.fields, index + 1, this.fields[index])
      this.$set(this.fields, index, tmp)
      this.emit()
    },
    emit() {
      // Backend expects {"fields":[{"name":"...","type":"...","nullable":true}]}
      this.$emit('input', JSON.stringify({ fields: this.fields }))
    }
  }
}
</script>
