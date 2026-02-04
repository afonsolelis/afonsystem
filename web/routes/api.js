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
  const issueFilter = buildIssueFilter(req.query);
  const projectFilter = req.query.project_id ? { project_id: parseInt(req.query.project_id) } : {};

  const [
    projectCount,
    commitCount,
    mrCount,
    issueCount,
    commitsPerStudent,
    commitsOverTime,
    linesPerStudent,
    mrStatus,
    issueStatus,
  ] = await Promise.all([
    db.collection('projects').countDocuments(projectFilter),
    db.collection('commits').countDocuments(commitFilter),
    db.collection('merge_requests').countDocuments(mrFilter),
    db.collection('issues').countDocuments(issueFilter),
    db.collection('commits').aggregate([
      { $match: commitFilter },
      { $group: { _id: '$author_name', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ]).toArray(),
    db.collection('commits').aggregate([
      { $match: commitFilter },
      { $addFields: { date: { $substr: ['$committed_date', 0, 10] } } },
      { $group: { _id: '$date', count: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ]).toArray(),
    db.collection('commits').aggregate([
      { $match: commitFilter },
      { $group: {
        _id: '$author_name',
        additions: { $sum: '$additions' },
        deletions: { $sum: '$deletions' },
      }},
      { $sort: { additions: -1 } },
    ]).toArray(),
    db.collection('merge_requests').aggregate([
      { $match: mrFilter },
      { $group: { _id: '$state', count: { $sum: 1 } } },
    ]).toArray(),
    db.collection('issues').aggregate([
      { $match: issueFilter },
      { $group: { _id: '$state', count: { $sum: 1 } } },
    ]).toArray(),
  ]);

  const filters = {};
  if (req.query.project_id) filters.project_id = parseInt(req.query.project_id);
  if (req.query.username) filters.username = req.query.username;
  if (req.query.from) filters.from = req.query.from;
  if (req.query.to) filters.to = req.query.to;

  res.json({
    exported_at: new Date().toISOString(),
    filters,
    summary: { projects: projectCount, commits: commitCount, merge_requests: mrCount, issues: issueCount },
    commits_per_student: commitsPerStudent.map(r => ({ student: r._id, commits: r.count })),
    commits_over_time: commitsOverTime.map(r => ({ date: r._id, commits: r.count })),
    lines_per_student: linesPerStudent.map(r => ({ student: r._id, additions: r.additions, deletions: r.deletions })),
    merge_request_status: mrStatus.map(r => ({ state: r._id, count: r.count })),
    issue_status: issueStatus.map(r => ({ state: r._id, count: r.count })),
  });
});

module.exports = router;
