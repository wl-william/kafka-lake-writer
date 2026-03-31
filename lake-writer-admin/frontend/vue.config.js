module.exports = {
  outputDir: '../src/main/resources/static',
  publicPath: '/',
  devServer: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:9080',
        changeOrigin: true
      }
    }
  }
}
