const express = require('express');
const router = express.Router();
const { getDB } = require('../config/db');

router.get('/:username', async (req, res) => {
  const db = getDB();
  const username = req.params.username;

  const memberDocs = await db.collection('members').find({ username }).toArray();
  if (memberDocs.length === 0) {
    const memberByName = await db.collection('members').find({ name: username }).toArray();
    if (memberByName.length === 0) return res.status(404).render('pages/404', { title: '404' });
    memberDocs.push(...memberByName);
  }

  const member = memberDocs[0];
  const projectIds = memberDocs.map(m => m.project_id);

  const [projects, commits, mergeRequests, issues] = await Promise.all([
    db.collection('projects').find({ project_id: { $in: projectIds } }).toArray(),
    db.collection('commits').find({ author_name: member.name, project_id: { $in: projectIds } }).sort({ committed_date: -1 }).toArray(),
    db.collection('merge_requests').find({ author_username: member.username, project_id: { $in: projectIds } }).toArray(),
    db.collection('issues').find({ author_username: member.username, project_id: { $in: projectIds } }).toArray(),
  ]);

  const projectMap = {};
  projects.forEach(p => { projectMap[p.project_id] = p; });

  const commitsByProject = {};
  commits.forEach(c => {
    if (!commitsByProject[c.project_id]) commitsByProject[c.project_id] = { count: 0, additions: 0, deletions: 0 };
    commitsByProject[c.project_id].count++;
    commitsByProject[c.project_id].additions += c.additions || 0;
    commitsByProject[c.project_id].deletions += c.deletions || 0;
  });

  res.render('pages/member', {
    title: member.name,
    member,
    memberDocs,
    projects,
    projectMap,
    commits,
    mergeRequests,
    issues,
    commitsByProject,
  });
});

module.exports = router;
