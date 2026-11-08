module.exports = {
  apps: [
    {
      name: 'blofin-vol',
      script: 'index.js',
      cwd: __dirname,
      // sửa config.json là pm2 tự restart với danh sách token mới
      watch: ['config.json'],
      autorestart: true,
      max_restarts: 50,
      restart_delay: 5000,
      max_memory_restart: '200M',
      time: true,
      env: { NODE_ENV: 'production' },
    },
  ],
};
