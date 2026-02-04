function buildCommitFilter(query) {
  const filter = {};
  if (query.project_id) filter.project_id = parseInt(query.project_id);
  if (query.username) filter.author_name = query.username;
  if (query.from || query.to) {
    filter.committed_date = {};
    if (query.from) filter.committed_date.$gte = query.from;
    if (query.to) filter.committed_date.$lte = query.to + 'T23:59:59Z';
  }
  return filter;
}

function buildMRFilter(query) {
  const filter = {};
  if (query.project_id) filter.project_id = parseInt(query.project_id);
  if (query.username) filter.author_username = query.username;
  if (query.from || query.to) {
    filter.created_at = {};
    if (query.from) filter.created_at.$gte = query.from;
    if (query.to) filter.created_at.$lte = query.to + 'T23:59:59Z';
  }
  return filter;
}

function buildIssueFilter(query) {
  const filter = {};
  if (query.project_id) filter.project_id = parseInt(query.project_id);
  if (query.username) filter.author_username = query.username;
  if (query.from || query.to) {
    filter.created_at = {};
    if (query.from) filter.created_at.$gte = query.from;
    if (query.to) filter.created_at.$lte = query.to + 'T23:59:59Z';
  }
  return filter;
}

module.exports = { buildCommitFilter, buildMRFilter, buildIssueFilter };
