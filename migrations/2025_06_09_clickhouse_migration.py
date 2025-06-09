#!/usr/bin/env python3

import clickhouse_connect

def create_tables(client):
    client.command("""
        CREATE TABLE IF NOT EXISTS repositories
        (
            repo_name String
        )
        ENGINE = MergeTree
        ORDER BY repo_name;
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS commits
        (
            sha String,
            message String,
            author String,
            email String,
            date DateTime,
            url String,
            repo_name String,
            PRIMARY KEY (sha, repo_name)
        )
        ENGINE = MergeTree
        ORDER BY (sha, repo_name);
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS pull_requests
        (
            number String,
            title String,
            author String,
            email String,
            created_at DateTime,
            state String,
            comments UInt32,
            review_comments UInt32,
            commits UInt32,
            url String,
            repo_name String,
            PRIMARY KEY (number, repo_name)
        )
        ENGINE = MergeTree
        ORDER BY (number, repo_name);
    """)

def main():
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='afonsystem'
    )

    create_tables(client)
    print("✅ Tabelas criadas com sucesso no ClickHouse.")

if __name__ == '__main__':
    main()
