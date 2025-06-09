#!/usr/bin/env python3

import time
import json
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv
import clickhouse_connect
import os

# Carrega variáveis do .env
load_dotenv(dotenv_path='.env')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPOS_JSON = os.getenv('GITREPOS')

# GitHub client
def get_github_client(token: str) -> Github:
    return Github(token)

# Repositórios do .env
def fetch_repositories_from_env() -> list[str]:
    try:
        return json.loads(REPOS_JSON)
    except Exception as e:
        raise ValueError("Erro ao decodificar GITREPOS no .env. Use formato JSON válido.") from e

# Conexão ClickHouse
def connect_clickhouse():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='afonsystem'
    )

# Commits do GitHub
def get_all_commits(repo_name: str, client: Github) -> pd.DataFrame:
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
    return pd.DataFrame(data)

# Pull requests do GitHub
def get_all_pull_requests(repo_name: str, client: Github) -> pd.DataFrame:
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
            'comments':        int(pr.comments),
            'review_comments': int(pr.review_comments),
            'commits':         len([c.sha for c in pr.get_commits()]),
            'url':             pr.html_url,
            'repo_name':       repo_name
        })
    return pd.DataFrame(data)

# Inserção direta no ClickHouse
def insert_clickhouse(client, df: pd.DataFrame, table: str):
    if df.empty:
        return 0

    if table == 'commits':
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        client.insert(table, df[['sha', 'message', 'author', 'email', 'date', 'url', 'repo_name']].values.tolist(),
                      column_names=['sha', 'message', 'author', 'email', 'date', 'url', 'repo_name'])

    elif table == 'pull_requests':
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        client.insert(table, df[['number', 'title', 'author', 'email', 'created_at', 'state',
                                 'comments', 'review_comments', 'commits', 'url', 'repo_name']].values.tolist(),
                      column_names=['number', 'title', 'author', 'email', 'created_at', 'state',
                                    'comments', 'review_comments', 'commits', 'url', 'repo_name'])
    return len(df)

# Main
def main():
    client = get_github_client(GITHUB_TOKEN)
    click_client = connect_clickhouse()
    repos = fetch_repositories_from_env()

    print(f"🔍 {len(repos)} repositórios carregados do .env.")
    start = time.time()

    for repo in repos:
        print(f"📦 Processando {repo}…")

        # Commits
        try:
            commits = get_all_commits(repo, client)
            count_new = insert_clickhouse(click_client, commits, 'commits')
            print(f"    • {count_new} commits enviados ao ClickHouse.")
        except GithubException as e:
            print(f"    ⚠️ Erro ao coletar commits: {e}")

        # Pull Requests
        try:
            prs = get_all_pull_requests(repo, client)
            count_pr = insert_clickhouse(click_client, prs, 'pull_requests')
            print(f"    • {count_pr} pull requests enviados ao ClickHouse.")
        except GithubException as e:
            print(f"    ⚠️ Erro ao coletar pull requests: {e}")

    print(f"✅ Coleta concluída em {time.time() - start:.2f}s")

if __name__ == '__main__':
    main()
