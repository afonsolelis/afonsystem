const path = require('path');
require('dotenv').config();

const express = require('express');
const { connectDB, closeDB } = require('./config/db');
const { startScheduler } = require('./services/scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));

app.use((req, res, next) => {
  res.locals.currentPath = req.path;
  next();
});

app.use('/', require('./routes/index'));
app.use('/projects', require('./routes/projects'));
app.use('/members', require('./routes/members'));
app.use('/api', require('./routes/api'));

app.use((req, res) => {
  res.status(404).render('pages/404', { title: '404' });
});

async function start() {
  await connectDB();
  startScheduler();
  app.listen(PORT, () => console.log(`Dashboard: http://localhost:${PORT}`));
}

start().catch(console.error);

process.on('SIGINT', async () => { await closeDB(); process.exit(0); });
process.on('SIGTERM', async () => { await closeDB(); process.exit(0); });
