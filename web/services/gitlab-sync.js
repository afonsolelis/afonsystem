const { getDB } = require('../config/db');

const GITLAB_URL = process.env.GITLAB_URL;
const GITLAB_PAT = process.env.GITLAB_PAT;

const headers = { 'PRIVATE-TOKEN': GITLAB_PAT };

async function paginatedGet(url, params = {}) {
  params.per_page = 100;
  params.page = 1;
  const results = [];

  while (true) {
    const qs = new URLSearchParams(params).toString();
    const res = await fetch(`${url}?${qs}`, { headers });
    if (!res.ok) {
      throw new Error(`GitLab API error: ${res.status} ${res.statusText} for ${url}`);
    }
    const data = await res.json();
    if (!data.length) break;
    results.push(...data);
    params.page++;
  }

  return results;
}

async function syncProjects() {
  const db = getDB();
  const projects = await paginatedGet(`${GITLAB_URL}/api/v4/projects`, { membership: true });
  console.log(`[gitlab-sync] Found ${projects.length} projects`);

  for (const project of projects) {
    const pid = project.id;
    const name = project.path_with_namespace;
    console.log(`[gitlab-sync] --- ${name} (id=${pid}) ---`);

    const doc = {
      project_id: project.id,
      name: project.name,
      path_with_namespace: project.path_with_namespace,
      description: project.description || null,
      created_at: project.created_at,
      default_branch: project.default_branch || null,
      web_url: project.web_url,
    };
    await db.collection('projects').updateOne(
      { project_id: doc.project_id },
      { $set: doc },
      { upsert: true }
    );
    console.log(`[gitlab-sync]   project saved`);

    let n;
    n = await syncMembers(pid);
    console.log(`[gitlab-sync]   ${n} members`);

    n = await syncCommits(pid);
    console.log(`[gitlab-sync]   ${n} commits`);

    n = await syncMergeRequests(pid);
    console.log(`[gitlab-sync]   ${n} merge requests`);

    n = await syncIssues(pid);
    console.log(`[gitlab-sync]   ${n} issues`);
  }

  return projects.length;
}

async function syncMembers(projectId) {
  const db = getDB();
  const members = await paginatedGet(`${GITLAB_URL}/api/v4/projects/${projectId}/members/all`);
  if (!members.length) return 0;

  const ops = members.map(m => ({
    updateOne: {
      filter: { project_id: projectId, user_id: m.id },
      update: {
        $set: {
          project_id: projectId,
          user_id: m.id,
          username: m.username,
          name: m.name,
          access_level: m.access_level,
        },
      },
      upsert: true,
    },
  }));

  await db.collection('members').bulkWrite(ops);
  return ops.length;
}

async function syncCommits(projectId) {
  const db = getDB();
  const commits = await paginatedGet(
    `${GITLAB_URL}/api/v4/projects/${projectId}/repository/commits`,
    { all: true, with_stats: true }
  );
  if (!commits.length) return 0;

  const ops = commits.map(c => {
    const stats = c.stats || {};
    return {
      updateOne: {
        filter: { project_id: projectId, sha: c.id },
        update: {
          $set: {
            project_id: projectId,
            sha: c.id,
            short_id: c.short_id,
            author_name: c.author_name,
            author_email: c.author_email,
            committed_date: c.committed_date,
            message: c.message,
            additions: stats.additions || 0,
            deletions: stats.deletions || 0,
            total: stats.total || 0,
          },
        },
        upsert: true,
      },
    };
  });

  await db.collection('commits').bulkWrite(ops);
  return ops.length;
}

async function syncMergeRequests(projectId) {
  const db = getDB();
  const mrs = await paginatedGet(
    `${GITLAB_URL}/api/v4/projects/${projectId}/merge_requests`,
    { state: 'all' }
  );
  if (!mrs.length) return 0;

  const ops = mrs.map(mr => {
    const author = mr.author || {};
    return {
      updateOne: {
        filter: { project_id: projectId, iid: mr.iid },
        update: {
          $set: {
            project_id: projectId,
            iid: mr.iid,
            title: mr.title,
            author_username: author.username || null,
            author_name: author.name || null,
            state: mr.state,
            created_at: mr.created_at,
            merged_at: mr.merged_at || null,
            closed_at: mr.closed_at || null,
            source_branch: mr.source_branch,
            target_branch: mr.target_branch,
            merge_commit_sha: mr.merge_commit_sha || null,
          },
        },
        upsert: true,
      },
    };
  });

  await db.collection('merge_requests').bulkWrite(ops);
  return ops.length;
}

async function syncIssues(projectId) {
  const db = getDB();
  const issues = await paginatedGet(
    `${GITLAB_URL}/api/v4/projects/${projectId}/issues`,
    { state: 'all' }
  );
  if (!issues.length) return 0;

  const ops = issues.map(issue => {
    const author = issue.author || {};
    const assignees = (issue.assignees || []).map(a => ({
      username: a.username,
      name: a.name,
    }));
    return {
      updateOne: {
        filter: { project_id: projectId, iid: issue.iid },
        update: {
          $set: {
            project_id: projectId,
            iid: issue.iid,
            title: issue.title,
            author_username: author.username || null,
            author_name: author.name || null,
            assignees,
            state: issue.state,
            labels: issue.labels || [],
            created_at: issue.created_at,
            closed_at: issue.closed_at || null,
          },
        },
        upsert: true,
      },
    };
  });

  await db.collection('issues').bulkWrite(ops);
  return ops.length;
}

async function runSync() {
  const startTime = Date.now();
  console.log(`[gitlab-sync] Starting sync at ${new Date().toISOString()}`);

  try {
    const projectCount = await syncProjects();

    const db = getDB();
    console.log('[gitlab-sync] === MongoDB summary (afonsystem) ===');
    for (const col of ['projects', 'members', 'commits', 'merge_requests', 'issues']) {
      const count = await db.collection(col).countDocuments({});
      console.log(`[gitlab-sync]   ${col}: ${count} documents`);
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[gitlab-sync] Sync completed: ${projectCount} projects in ${elapsed}s`);
  } catch (err) {
    console.error('[gitlab-sync] Sync failed:', err.message);
  }
}

module.exports = { runSync };
