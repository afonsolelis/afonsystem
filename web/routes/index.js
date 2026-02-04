const express = require('express');
const router = express.Router();
const { getDB } = require('../config/db');
const { buildCommitFilter, buildMRFilter, buildIssueFilter } = require('../utils/filters');

router.get('/', async (req, res) => {
  const db = getDB();

  const commitFilter = buildCommitFilter(req.query);
  const mrFilter = buildMRFilter(req.query);
  const issueFilter = buildIssueFilter(req.query);
  const projectFilter = req.query.project_id ? { project_id: parseInt(req.query.project_id) } : {};

  const [projects, commits, mergeRequests, issues, projectsList, membersList] = await Promise.all([
    db.collection('projects').countDocuments(projectFilter),
    db.collection('commits').countDocuments(commitFilter),
    db.collection('merge_requests').countDocuments(mrFilter),
    db.collection('issues').countDocuments(issueFilter),
    db.collection('projects').find({}, { projection: { project_id: 1, name: 1, path_with_namespace: 1, default_branch: 1, created_at: 1 } }).toArray(),
    db.collection('members').distinct('name', { access_level: { $lt: 50 } }),
  ]);

  res.render('pages/dashboard', {
    title: 'Dashboard',
    stats: { projects, commits, mergeRequests, issues },
    projectsList,
    membersList,
    selectedProject: req.query.project_id || '',
    selectedUsername: req.query.username || '',
    selectedFrom: req.query.from || '',
    selectedTo: req.query.to || '',
  });
});

module.exports = router;
