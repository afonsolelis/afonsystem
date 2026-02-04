const express = require('express');
const router = express.Router();
const { getDB } = require('../config/db');

router.get('/:id', async (req, res) => {
  const db = getDB();
  const projectId = parseInt(req.params.id);

  const [project, members, commits, mergeRequests, issues] = await Promise.all([
    db.collection('projects').findOne({ project_id: projectId }),
    db.collection('members').find({ project_id: projectId }).toArray(),
    db.collection('commits').find({ project_id: projectId }).sort({ committed_date: -1 }).toArray(),
    db.collection('merge_requests').find({ project_id: projectId }).toArray(),
    db.collection('issues').find({ project_id: projectId }).toArray(),
  ]);

  if (!project) return res.status(404).render('pages/404', { title: '404' });

  const commitsByAuthor = {};
  commits.forEach(c => {
    if (!commitsByAuthor[c.author_name]) {
      commitsByAuthor[c.author_name] = { count: 0, additions: 0, deletions: 0 };
    }
    commitsByAuthor[c.author_name].count++;
    commitsByAuthor[c.author_name].additions += c.additions || 0;
    commitsByAuthor[c.author_name].deletions += c.deletions || 0;
  });

  const mrStatusCounts = { opened: 0, merged: 0, closed: 0 };
  mergeRequests.forEach(mr => { mrStatusCounts[mr.state] = (mrStatusCounts[mr.state] || 0) + 1; });

  const issueStatusCounts = { opened: 0, closed: 0 };
  issues.forEach(i => { issueStatusCounts[i.state] = (issueStatusCounts[i.state] || 0) + 1; });

  res.render('pages/project', {
    title: project.name,
    project,
    members,
    commits,
    mergeRequests,
    issues,
    commitsByAuthor,
    mrStatusCounts,
    issueStatusCounts,
  });
});

module.exports = router;
