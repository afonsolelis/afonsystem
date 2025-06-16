import streamlit as st
import time
import json
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv
import clickhouse_connect
import os

# 🔧 Configuração inicial
st.set_page_config(page_title="GitHub ETL para ClickHouse", layout="wide")
load_dotenv(dotenv_path=".env")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPOS_JSON = os.getenv("GITREPOS")

# 🔗 Conexão com ClickHouse
def connect_clickhouse():
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="afonsystem"
    )

# 🔗 Conexão com GitHub
def get_github_client(token: str) -> Github:
    return Github(token)

# 🔗 Busca dos repositórios no .env
def fetch_repositories_from_env() -> list[str]:
    try:
        return json.loads(REPOS_JSON)
    except Exception as e:
        st.error("❌ Erro ao ler GITREPOS no .env. Verifique o formato JSON.")
        raise e

# 🔍 Busca SHAs existentes no ClickHouse
def get_existing_commits(client, repo_name: str) -> set:
    try:
        result = client.query(f"SELECT sha FROM commits WHERE repo_name = '{repo_name}'")
        return set(row[0] for row in result.result_rows)
    except Exception:
        return set()

# 🔍 Busca PRs existentes no ClickHouse  
def get_existing_prs(client, repo_name: str) -> set:
    try:
        result = client.query(f"SELECT number FROM pull_requests WHERE repo_name = '{repo_name}'")
        return set(str(row[0]) for row in result.result_rows)
    except Exception:
        return set()

# 🔥 Coleta de dados de commits (APENAS NOVOS)
def get_new_commits(repo_name: str, github_client: Github, click_client) -> pd.DataFrame:
    existing_shas = get_existing_commits(click_client, repo_name)
    st.write(f"📊 {len(existing_shas)} commits já existem no banco")
    
    data = []
    repo = github_client.get_repo(repo_name)
    
    for c in repo.get_commits():
        if c.sha not in existing_shas:  # ✅ SÓ ADICIONA SE NÃO EXISTIR
            author = c.commit.author or {}
            data.append({
                "sha": c.sha,
                "message": c.commit.message,
                "author": getattr(author, "name", "") or "",
                "email": getattr(author, "email", "") or "",
                "date": getattr(author, "date", None).isoformat() if getattr(author, "date", None) else None,
                "url": c.html_url,
                "repo_name": repo_name
            })
    
    st.write(f"🆕 {len(data)} novos commits encontrados")
    return pd.DataFrame(data)

# 🔥 Coleta de dados de pull requests (APENAS NOVOS)
def get_new_pull_requests(repo_name: str, github_client: Github, click_client) -> pd.DataFrame:
    existing_prs = get_existing_prs(click_client, repo_name)
    st.write(f"📊 {len(existing_prs)} PRs já existem no banco")
    
    data = []
    repo = github_client.get_repo(repo_name)
    
    for pr in repo.get_pulls(state="all", sort="created", direction="desc"):
        if str(pr.number) not in existing_prs:  # ✅ SÓ ADICIONA SE NÃO EXISTIR
            data.append({
                "number": str(pr.number),
                "title": pr.title,
                "author": pr.user.login,
                "email": pr.user.email or "",
                "created_at": pr.created_at.isoformat() if pr.created_at else None,
                "state": pr.state,
                "comments": int(pr.comments),
                "review_comments": int(pr.review_comments),
                "commits": len(list(pr.get_commits())),
                "url": pr.html_url,
                "repo_name": repo_name
            })
    
    st.write(f"🆕 {len(data)} novos PRs encontrados")
    return pd.DataFrame(data)

# 🚚 Inserção no ClickHouse
def insert_clickhouse(client, df: pd.DataFrame, table: str):
    if df.empty:
        st.info("📭 Nenhum dado novo para inserir")
        return 0

    if table == "commits":
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        client.insert(
            table,
            df[["sha", "message", "author", "email", "date", "url", "repo_name"]].values.tolist(),
            column_names=["sha", "message", "author", "email", "date", "url", "repo_name"]
        )

    elif table == "pull_requests":
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        client.insert(
            table,
            df[["number", "title", "author", "email", "created_at", "state",
                "comments", "review_comments", "commits", "url", "repo_name"]].values.tolist(),
            column_names=["number", "title", "author", "email", "created_at", "state",
                          "comments", "review_comments", "commits", "url", "repo_name"]
        )

    return len(df)

# 🔧 Função para otimizar tabelas (opcional agora)
def optimize_tables(client):
    with st.spinner("🧹 Otimizando tabelas..."):
        client.command("OPTIMIZE TABLE commits FINAL")
        client.command("OPTIMIZE TABLE pull_requests FINAL")
    st.success("🧹 Otimização concluída!")

# 🖥️ Interface Streamlit
st.title("🚀 ETL de GitHub para ClickHouse (SEM duplicados)")

with st.sidebar:
    st.subheader("⚙️ Filtro de Repositórios")
    repos = fetch_repositories_from_env()

    search_term = st.text_input("🔍 Buscar repositórios:")
    if search_term:
        filtered_repos = [r for r in repos if search_term.lower() in r.lower()]
    else:
        filtered_repos = repos

    selected_repos = st.multiselect(
        "Selecione os repositórios:",
        filtered_repos,
        default=filtered_repos
    )

    start_button = st.button("🔄 Atualizar Dados")

if start_button and selected_repos:
    st.info(f"🔄 Iniciando atualização de {len(selected_repos)} repositórios...")
    github_client = get_github_client(GITHUB_TOKEN)
    click_client = connect_clickhouse()
    start_time = time.time()

    total_new_commits = 0
    total_new_prs = 0

    for repo in selected_repos:
        with st.expander(f"📦 {repo}"):
            try:
                st.write("➡️ Verificando commits existentes e coletando novos...")
                commits = get_new_commits(repo, github_client, click_client)
                n_commits = insert_clickhouse(click_client, commits, "commits")
                total_new_commits += n_commits
                if n_commits > 0:
                    st.success(f"✅ {n_commits} novos commits inseridos!")

            except GithubException as e:
                st.error(f"⚠️ Erro ao coletar commits: {e}")

            try:
                st.write("➡️ Verificando PRs existentes e coletando novos...")
                prs = get_new_pull_requests(repo, github_client, click_client)
                n_prs = insert_clickhouse(click_client, prs, "pull_requests")
                total_new_prs += n_prs
                if n_prs > 0:
                    st.success(f"✅ {n_prs} novos PRs inseridos!")

            except GithubException as e:
                st.error(f"⚠️ Erro ao coletar pull requests: {e}")

    # Otimização opcional (só se houver dados novos)
    if total_new_commits > 0 or total_new_prs > 0:
        optimize_tables(click_client)

    st.success(f"""
    🏁 **ETL Concluído em {time.time() - start_time:.2f} segundos!**
    
    📊 **Resumo:**
    - 🆕 {total_new_commits} novos commits inseridos
    - 🆕 {total_new_prs} novos pull requests inseridos
    """)

else:
    st.warning("⚠️ Selecione pelo menos um repositório para atualizar.")