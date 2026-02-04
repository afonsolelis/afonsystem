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

  const ccFilter = (prefix) => ({
    ...commitFilter,
    message: { $regex: `^${prefix}(\\(.+\\))?:`, $options: 'i' },
  });

  const [projects, commits, mergeRequests, issues, projectsList, membersList,
    ccFeat, ccFix, ccDocs, ccChore] = await Promise.all([
    db.collection('projects').countDocuments(projectFilter),
    db.collection('commits').countDocuments(commitFilter),
    db.collection('merge_requests').countDocuments(mrFilter),
    db.collection('issues').countDocuments(issueFilter),
    db.collection('projects').find({}, { projection: { project_id: 1, name: 1, path_with_namespace: 1, default_branch: 1, created_at: 1 } }).toArray(),
    db.collection('members').distinct('name', { access_level: { $lt: 50 } }),
    db.collection('commits').countDocuments(ccFilter('feat')),
    db.collection('commits').countDocuments(ccFilter('fix')),
    db.collection('commits').countDocuments(ccFilter('docs')),
    db.collection('commits').countDocuments(ccFilter('chore')),
  ]);

  res.render('pages/dashboard', {
    title: 'Dashboard',
    stats: { projects, commits, mergeRequests, issues, ccFeat, ccFix, ccDocs, ccChore },
    projectsList,
    membersList,
    selectedProject: req.query.project_id || '',
    selectedUsername: req.query.username || '',
    selectedFrom: req.query.from || '',
    selectedTo: req.query.to || '',
  });
});

module.exports = router;
