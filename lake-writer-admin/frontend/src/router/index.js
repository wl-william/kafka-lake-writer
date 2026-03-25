import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: () => import('../views/Dashboard.vue')
  },
  {
    path: '/configs',
    name: 'ConfigList',
    component: () => import('../views/ConfigList.vue')
  },
  {
    path: '/configs/new',
    name: 'ConfigCreate',
    component: () => import('../views/ConfigEdit.vue')
  },
  {
    path: '/configs/:id/edit',
    name: 'ConfigEdit',
    component: () => import('../views/ConfigEdit.vue')
  },
  {
    path: '/configs/:id/monitor',
    name: 'MonitorDetail',
    component: () => import('../views/MonitorDetail.vue')
  },
  {
    path: '/nodes',
    name: 'NodeList',
    component: () => import('../views/NodeList.vue')
  },
  {
    path: '/changelog',
    name: 'ChangeLog',
    component: () => import('../views/ChangeLog.vue')
  }
]

const router = new VueRouter({
  mode: 'history',
  base: process.env.BASE_URL,
  routes
})

export default router
