/**
 * PM2 Ecosystem Configuration for auto-market-index-dashboard
 *
 * Usage on VPS:
 *   pm2 start ecosystem.config.js
 *
 * OOM guard: --max-memory-restart 150M (per Thread 31 full-send plan)
 */

module.exports = {
  apps: [
    {
      name: 'auto-dashboard',
      script: 'server.js',
      cwd: '/home/support/auto-market-index-dashboard',
      env: {
        PORT: 5006,
        NODE_ENV: 'production'
      },
      watch: false,
      max_memory_restart: '150M',
      error_file: '/home/support/auto-market-index-dashboard/logs/error.log',
      out_file:   '/home/support/auto-market-index-dashboard/logs/out.log',
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z'
    }
  ]
};
