import Vue from 'vue'
import VueRouter from 'vue-router'
import { isLoggedIn } from '../api/auth'

Vue.use(VueRouter)

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('../views/Login.vue'),
    meta: { public: true }
  },
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

router.beforeEach((to, from, next) => {
  if (to.meta.public || isLoggedIn()) {
    next()
  } else {
    next('/login')
  }
})

export default router
