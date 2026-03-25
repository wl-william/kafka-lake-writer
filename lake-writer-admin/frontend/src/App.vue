<template>
  <div id="app">
    <el-container style="height: 100vh;">
      <el-aside width="200px" style="background: #304156;">
        <div class="logo">
          <span>Lake Writer</span>
        </div>
        <el-menu
          :router="true"
          :default-active="$route.path"
          background-color="#304156"
          text-color="#bfcbd9"
          active-text-color="#409EFF">
          <el-menu-item index="/">
            <i class="el-icon-monitor"></i>
            <span>概览</span>
          </el-menu-item>
          <el-menu-item index="/configs">
            <i class="el-icon-setting"></i>
            <span>配置管理</span>
          </el-menu-item>
          <el-menu-item index="/nodes">
            <i class="el-icon-cpu"></i>
            <span>节点状态</span>
          </el-menu-item>
          <el-menu-item index="/changelog">
            <i class="el-icon-document"></i>
            <span>变更日志</span>
          </el-menu-item>
        </el-menu>
      </el-aside>
      <el-container>
        <el-header style="background: #fff; border-bottom: 1px solid #e6e6e6; line-height: 60px; padding: 0 20px;">
          <span style="font-size: 16px; font-weight: 600; color: #333;">Kafka Lake Writer 管理平台</span>
        </el-header>
        <el-main style="background: #f0f2f5; padding: 20px;">
          <router-view></router-view>
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>

<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #fff;
  font-size: 18px;
  font-weight: 700;
  border-bottom: 1px solid #3d4f6b;
}
</style>
