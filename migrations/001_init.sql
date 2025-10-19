CREATE DATABASE IF NOT EXISTS afonsystem;

CREATE TABLE IF NOT EXISTS afonsystem.commits
(
    snapshot_id String,
    quarter LowCardinality(String),
    repository LowCardinality(String),
    sha FixedString(40),
    message String,
    author String,
    committed_at DateTime64(3, 'UTC'),
    url String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(committed_at)
ORDER BY (repository, committed_at, sha, snapshot_id);

CREATE TABLE IF NOT EXISTS afonsystem.pull_requests
(
    snapshot_id String,
    quarter LowCardinality(String),
    repository LowCardinality(String),
    number UInt32,
    title String,
    author String,
    state LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    url String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (repository, created_at, number, snapshot_id);


