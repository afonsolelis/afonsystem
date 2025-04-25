#!/usr/bin/env python3

import time
import json
import duckdb
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv
import os

# Carrega variáveis do arquivo .env na raiz do projeto
load_dotenv(dotenv_path='.env')

# Lê o token do GitHub a partir da variável de ambiente
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
DB_FILE = 'duckdb_exports/default.duckdb'

def get_github_client(token: str) -> Github:
    return Github(token)

def fetch_repositories(client: Github) -> list[str]:
    user = client.get_user()
    return [
        repo.full_name
        for repo in user.get_repos()
        if 'PUBLICO' in repo.full_name
    ]

def get_all_commits(repo_name: str, client: Github) -> list[dict]:
    data = []
    repo = client.get_repo(repo_name)
    for c in repo.get_commits():
        author = c.commit.author or {}
        data.append({
            'sha':       c.sha,
            'message':   c.commit.message,
            'author':    getattr(author, 'name', '') or '',
            'email':     getattr(author, 'email', '') or '',
            'date':      getattr(author, 'date', None).isoformat() if getattr(author, 'date', None) else None,
            'url':       c.html_url,
            'repo_name': repo_name
        })
    return data

def get_all_pull_requests(repo_name: str, client: Github) -> list[dict]:
    data = []
    repo = client.get_repo(repo_name)
    for pr in repo.get_pulls(state='all', sort='created', direction='desc'):
        data.append({
            'number':          str(pr.number),
            'title':           pr.title,
            'author':          pr.user.login,
            'email':           pr.user.email or '',
            'created_at':      pr.created_at.isoformat() if pr.created_at else None,
            'state':           pr.state,
            'comments':        str(pr.comments),
            'review_comments': str(pr.review_comments),
            'commits':         json.dumps([c.sha for c in pr.get_commits()]),
            'url':             pr.html_url,
            'repo_name':       repo_name
        })
    return data

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

def get_existing_set(conn, table: str, column: str, repo_name: str = None) -> set:
    if repo_name:
        rows = conn.execute(
            f"SELECT {column} FROM {table} WHERE repo_name = ?",
            [repo_name]
        ).fetchall()
    else:
        rows = conn.execute(f"SELECT {column} FROM {table}").fetchall()
    return {row[0] for row in rows}

def insert_new_repositories(conn, repo_names: list[str]):
    existing = get_existing_set(conn, 'repositories', 'repo_name')
    new = [(r,) for r in repo_names if r not in existing]
    if new:
        conn.executemany("INSERT INTO repositories(repo_name) VALUES (?)", new)
        print(f"  ➤ {len(new)} novos repositórios adicionados.")
    else:
        print("  ➤ Nenhum repositório novo para adicionar.")

def insert_new_records(conn, df: pd.DataFrame, table: str, key_col: str, repo_name: str):
    if df.empty:
        return 0
    existing = get_existing_set(conn, table, key_col, repo_name)
    df_new = df[~df[key_col].isin(existing)]
    if df_new.empty:
        return 0
    tmp = f"tmp_{table}"
    conn.register(tmp, df_new)
    conn.execute(f"INSERT INTO {table} SELECT * FROM {tmp}")
    conn.unregister(tmp)
    return len(df_new)

def main():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    client = get_github_client(GITHUB_TOKEN)
    repos = fetch_repositories(client)
    print(f"🔍 {len(repos)} repositórios filtrados.")

    conn = duckdb.connect(database=DB_FILE)
    ensure_tables(conn)
    insert_new_repositories(conn, repos)

    start = time.time()
    for repo in repos:
        print(f"📦 Processando {repo}…")

        # Processa commits
        try:
            commits = pd.DataFrame(get_all_commits(repo, client))
            count_new = insert_new_records(conn, commits, 'commits', 'sha', repo)
            print(f"    • {count_new} commits novos inseridos.")
        except GithubException as e:
            if 'Git Repository is empty' in str(e):
                print(f"    ⚠️ Repositório vazio, pulando commits.")
            else:
                print(f"    ⚠️ Erro ao coletar commits: {e}")

        # Processa pull requests
        try:
            prs = pd.DataFrame(get_all_pull_requests(repo, client))
            count_pr = insert_new_records(conn, prs, 'pull_requests', 'number', repo)
            print(f"    • {count_pr} pull requests novos inseridos.")
        except GithubException as e:
            if 'Git Repository is empty' in str(e):
                print(f"    ⚠️ Repositório vazio, pulando pull requests.")
            else:
                print(f"    ⚠️ Erro ao coletar pull requests: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Coleta concluída em {time.time() - start:.2f}s")

if __name__ == '__main__':
    main()
