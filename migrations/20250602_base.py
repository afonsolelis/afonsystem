#!/usr/bin/env python3

import duckdb
import os

DB_FILE = 'duckdb_exports/default.duckdb'

def ensure_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repositories (
            repo_name TEXT PRIMARY KEY
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commits (
            sha TEXT,
            message TEXT,
            author TEXT,
            email TEXT,
            date TEXT,
            url TEXT,
            repo_name TEXT,
            PRIMARY KEY (sha, repo_name)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pull_requests (
            number TEXT,
            title TEXT,
            author TEXT,
            email TEXT,
            created_at TEXT,
            state TEXT,
            comments TEXT,
            review_comments TEXT,
            commits TEXT,
            url TEXT,
            repo_name TEXT,
            PRIMARY KEY (number, repo_name)
        );
    """)

def main():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = duckdb.connect(database=DB_FILE)
    ensure_tables(conn)
    conn.close()
    print("✅ Tabelas criadas com sucesso.")

if __name__ == '__main__':
    main()
