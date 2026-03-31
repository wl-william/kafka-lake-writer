import Vue from 'vue'
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'
import App from './App.vue'
import router from './router'
import { setupAxiosAuth } from './api/auth'

Vue.use(ElementUI, { size: 'medium' })
Vue.config.productionTip = false

// Install axios interceptors for HTTP Basic auth
setupAxiosAuth()

new Vue({
  router,
  render: h => h(App)
}).$mount('#app')
