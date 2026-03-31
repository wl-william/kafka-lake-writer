<template>
  <div style="display:flex;justify-content:center;align-items:center;height:100vh;background:#f5f7fa;">
    <el-card style="width:360px;">
      <h3 style="text-align:center;margin-bottom:20px;">Lake Writer Admin</h3>
      <el-form @submit.native.prevent="handleLogin">
        <el-form-item>
          <el-input v-model="username" prefix-icon="el-icon-user" placeholder="Username" />
        </el-form-item>
        <el-form-item>
          <el-input v-model="password" prefix-icon="el-icon-lock" type="password"
                    placeholder="Password" @keyup.enter.native="handleLogin" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" style="width:100%;" :loading="loading" @click="handleLogin">
            Login
          </el-button>
        </el-form-item>
        <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" />
      </el-form>
    </el-card>
  </div>
</template>

<script>
import { login } from '../api/auth'

export default {
  name: 'UserLogin',
  data() {
    return { username: '', password: '', loading: false, error: '' }
  },
  methods: {
    handleLogin() {
      if (!this.username || !this.password) {
        this.error = 'Please enter username and password'
        return
      }
      this.loading = true
      this.error = ''
      login(this.username, this.password)
        .then(() => {
          this.$router.push('/')
        })
        .catch(() => {
          this.error = 'Invalid credentials'
        })
        .finally(() => { this.loading = false })
    }
  }
}
</script>
