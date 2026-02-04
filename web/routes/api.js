const express = require('express');
const router = express.Router();
const { getDB } = require('../config/db');
const { buildCommitFilter, buildMRFilter, buildIssueFilter } = require('../utils/filters');

router.get('/commits-per-student', async (req, res) => {
  const db = getDB();
  const filter = buildCommitFilter(req.query);
  const result = await db.collection('commits').aggregate([
    { $match: filter },
    { $group: { _id: '$author_name', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  res.json({
    labels: result.map(r => r._id),
    datasets: [{ label: 'Commits', data: result.map(r => r.count) }],
  });
});

router.get('/commits-over-time', async (req, res) => {
  const db = getDB();
  const filter = buildCommitFilter(req.query);
  const result = await db.collection('commits').aggregate([
    { $match: filter },
    { $addFields: { date: { $substr: ['$committed_date', 0, 10] } } },
    { $group: { _id: '$date', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();

  res.json({
    labels: result.map(r => r._id),
    datasets: [{ label: 'Commits', data: result.map(r => r.count), fill: false }],
  });
});

router.get('/lines-per-student', async (req, res) => {
  const db = getDB();
  const filter = buildCommitFilter(req.query);
  const result = await db.collection('commits').aggregate([
    { $match: filter },
    { $group: {
      _id: '$author_name',
      additions: { $sum: '$additions' },
      deletions: { $sum: '$deletions' },
    }},
    { $sort: { additions: -1 } },
  ]).toArray();

  res.json({
    labels: result.map(r => r._id),
    datasets: [
      { label: 'Adicionadas', data: result.map(r => r.additions), backgroundColor: '#198754' },
      { label: 'Removidas', data: result.map(r => r.deletions), backgroundColor: '#dc3545' },
    ],
  });
});

router.get('/mr-status', async (req, res) => {
  const db = getDB();
  const filter = buildMRFilter(req.query);
  const result = await db.collection('merge_requests').aggregate([
    { $match: filter },
    { $group: { _id: '$state', count: { $sum: 1 } } },
  ]).toArray();

  const statusColors = { opened: '#0d6efd', merged: '#198754', closed: '#dc3545' };
  res.json({
    labels: result.map(r => r._id),
    datasets: [{
      data: result.map(r => r.count),
      backgroundColor: result.map(r => statusColors[r._id] || '#6c757d'),
    }],
  });
});

router.get('/issue-status', async (req, res) => {
  const db = getDB();
  const filter = buildIssueFilter(req.query);
  const result = await db.collection('issues').aggregate([
    { $match: filter },
    { $group: { _id: '$state', count: { $sum: 1 } } },
  ]).toArray();

  const statusColors = { opened: '#0d6efd', closed: '#198754' };
  res.json({
    labels: result.map(r => r._id),
    datasets: [{
      data: result.map(r => r.count),
      backgroundColor: result.map(r => statusColors[r._id] || '#6c757d'),
    }],
  });
});

router.get('/export', async (req, res) => {
  const db = getDB();
  const commitFilter = buildCommitFilter(req.query);
  const mrFilter = buildMRFilter(req.query);
  const noId = { projection: { _id: 0 } };

  const [commits, mergeRequests] = await Promise.all([
    db.collection('commits').find(commitFilter, noId).sort({ committed_date: -1 }).toArray(),
    db.collection('merge_requests').find(mrFilter, noId).sort({ created_at: -1 }).toArray(),
  ]);

  const filters = {};
  if (req.query.project_id) filters.project_id = parseInt(req.query.project_id);
  if (req.query.username) filters.username = req.query.username;
  if (req.query.from) filters.from = req.query.from;
  if (req.query.to) filters.to = req.query.to;

  res.json({
    exported_at: new Date().toISOString(),
    filters,
    total_commits: commits.length,
    total_merge_requests: mergeRequests.length,
    commits,
    merge_requests: mergeRequests,
  });
});

module.exports = router;
