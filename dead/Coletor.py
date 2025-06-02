import streamlit as st
import time
import json
import duckdb
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(dotenv_path='.env')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
DB_FILE = 'duckdb_exports/default.duckdb'

# Funções do coletor
def get_github_client(token: str):
    return Github(token)

def fetch_repositories(client, filtro: str):
    user = client.get_user()
    return [
        repo.full_name
        for repo in user.get_repos()
        if filtro in repo.full_name
    ]

def get_all_commits(repo_name, client):
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

def get_all_pull_requests(repo_name, client):
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

def get_existing_set(conn, table, column, repo_name=None):
    if repo_name:
        rows = conn.execute(
            f"SELECT {column} FROM {table} WHERE repo_name = ?",
            [repo_name]
        ).fetchall()
    else:
        rows = conn.execute(f"SELECT {column} FROM {table}").fetchall()
    return {row[0] for row in rows}

def insert_new_repositories(conn, repo_names):
    existing = get_existing_set(conn, 'repositories', 'repo_name')
    new = [(r,) for r in repo_names if r not in existing]
    if new:
        conn.executemany("INSERT INTO repositories(repo_name) VALUES (?)", new)
        st.write(f"  ➤ {len(new)} novos repositórios adicionados.")
    else:
        st.write("  ➤ Nenhum repositório novo para adicionar.")

def insert_new_records(conn, df, table, key_col, repo_name):
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

def update_github_database(repos):
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    client = get_github_client(GITHUB_TOKEN)

    st.write(f"🔍 {len(repos)} repositórios filtrados.")
    conn = duckdb.connect(database=DB_FILE)
    ensure_tables(conn)
    insert_new_repositories(conn, repos)

    start = time.time()
    for repo in repos:
        st.write(f"📦 Processando {repo}…")
        # Commits
        try:
            commits = pd.DataFrame(get_all_commits(repo, client))
            count_new = insert_new_records(conn, commits, 'commits', 'sha', repo)
            st.write(f"    • {count_new} commits novos inseridos.")
        except GithubException as e:
            if 'Git Repository is empty' in str(e):
                st.warning(f"    ⚠️ Repositório vazio, pulando commits.")
            else:
                st.warning(f"    ⚠️ Erro ao coletar commits: {e}")

        # Pull requests
        try:
            prs = pd.DataFrame(get_all_pull_requests(repo, client))
            count_pr = insert_new_records(conn, prs, 'pull_requests', 'number', repo)
            st.write(f"    • {count_pr} pull requests novos inseridos.")
        except GithubException as e:
            if 'Git Repository is empty' in str(e):
                st.warning(f"    ⚠️ Repositório vazio, pulando pull requests.")
            else:
                st.warning(f"    ⚠️ Erro ao coletar pull requests: {e}")

    conn.commit()
    conn.close()
    st.success(f"✅ Coleta concluída em {time.time() - start:.2f}s")

# ================= INTERFACE ===================

st.set_page_config(page_title="Coletor GitHub", layout="centered")
st.title("Coletor de Dados do GitHub")
st.write("""
1. Informe abaixo um filtro de texto para os nomes dos repositórios (ex: 'PUBLICO', '2024-1A' etc)
2. Clique em 'Buscar repositórios';
3. Veja a lista de repositórios encontrados;
4. Clique em 'Atualizar banco de dados' para iniciar a coleta.
""")

with st.form("filtro_repos"):
    filtro = st.text_input("Filtro para nome do repositório (case sensitive)", "PUBLICO")
    buscar = st.form_submit_button("Buscar repositórios")

if 'repos_filtrados' not in st.session_state:
    st.session_state['repos_filtrados'] = []

if buscar:
    with st.spinner("Buscando repositórios do GitHub…"):
        try:
            client = get_github_client(GITHUB_TOKEN)
            repos_filtrados = fetch_repositories(client, filtro)
            st.session_state['repos_filtrados'] = repos_filtrados
        except Exception as e:
            st.error(f"Erro ao buscar repositórios: {e}")
            st.stop()

repos_filtrados = st.session_state['repos_filtrados']

if repos_filtrados:
    st.success(f"Encontrados {len(repos_filtrados)} repositórios:")
    st.write(repos_filtrados)

    if st.button("Atualizar banco de dados com estes repositórios"):
        with st.spinner("Atualizando banco de dados com os repositórios filtrados…"):
            try:
                update_github_database(repos_filtrados)
            except Exception as e:
                st.error(f"Erro ao atualizar banco de dados: {e}")

else:
    st.info("Use o filtro acima e clique em 'Buscar repositórios' para listar primeiro os repositórios que serão coletados.")
