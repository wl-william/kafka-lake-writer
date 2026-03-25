<template>
  <div ref="chart" style="width: 100%; height: 280px;"></div>
</template>

<script>
import echarts from 'echarts'

export default {
  name: 'ConsumerLagChart',
  props: {
    topic: { type: String, default: '' }
  },
  data() {
    return {
      chart: null,
      timer: null,
      lagHistory: []
    }
  },
  mounted() {
    this.chart = echarts.init(this.$refs.chart)
    this.chart.setOption(this.buildOption([]))
    this.tick()
    this.timer = setInterval(this.tick, 15000)
  },
  beforeDestroy() {
    clearInterval(this.timer)
    if (this.chart) this.chart.dispose()
  },
  methods: {
    tick() {
      // In a real system this would call a lag API.
      // Here we simulate to keep the chart functional.
      const now = new Date().toLocaleTimeString('zh-CN')
      this.lagHistory.push({ time: now, lag: Math.floor(Math.random() * 500) })
      if (this.lagHistory.length > 20) this.lagHistory.shift()
      this.chart.setOption(this.buildOption(this.lagHistory))
    },
    buildOption(data) {
      return {
        tooltip: { trigger: 'axis' },
        xAxis: {
          type: 'category',
          data: data.map(d => d.time),
          axisLabel: { rotate: 30, fontSize: 10 }
        },
        yAxis: {
          type: 'value',
          name: 'Lag (条)',
          nameTextStyle: { fontSize: 11 }
        },
        series: [{
          name: 'Consumer Lag',
          type: 'line',
          smooth: true,
          data: data.map(d => d.lag),
          itemStyle: { color: '#409EFF' },
          areaStyle: { color: 'rgba(64,158,255,0.1)' }
        }],
        grid: { left: '10%', right: '5%', top: '10%', bottom: '20%' }
      }
    }
  }
}
</script>
