module.exports = {
  apps: [
    {
      name: 'blofin-vol',
      script: 'index.js',
      cwd: __dirname,
      // pm2 restarts automatically when config.json changes
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
