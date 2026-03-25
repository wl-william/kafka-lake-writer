<template>
  <div style="font-size: 13px; color: #606266; padding: 6px 10px; background: #f5f7fa; border-radius: 4px; font-family: monospace;">
    {{ preview }}
  </div>
</template>

<script>
export default {
  name: 'PathPreview',
  props: {
    template: { type: String, default: '' },
    topic: { type: String, default: 'sample_topic' }
  },
  computed: {
    preview() {
      if (!this.template) return '(请输入路径模板)'
      const now = new Date()
      const pad = n => String(n).padStart(2, '0')
      const dt = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
      const hour = pad(now.getHours())
      const minute = pad(now.getMinutes())
      const date = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
      return this.template
        .replace(/\{topic\}/g, this.topic || 'sample_topic')
        .replace(/\{dt\}/g, dt)
        .replace(/\{hour\}/g, hour)
        .replace(/\{minute\}/g, minute)
        .replace(/\{date\}/g, date)
        .replace(/\{partition\}/g, '0')
    }
  }
}
</script>
