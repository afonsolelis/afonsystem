const cron = require('node-cron');
const { runSync } = require('./gitlab-sync');

function startScheduler() {
  cron.schedule('0 0 * * *', () => {
    console.log('[scheduler] Midnight cron triggered');
    runSync();
  });
  console.log('[scheduler] GitLab sync scheduled for midnight (0 0 * * *)');
}

module.exports = { startScheduler };
