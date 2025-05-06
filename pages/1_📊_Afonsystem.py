import streamlit as st
import pandas as pd
import plotly.express as px
import os
import duckdb

# Layout da página
st.set_page_config(layout="wide")

# Caminho do banco DuckDB
db_path = os.path.join("duckdb_exports", "default.duckdb")

# Conectar ao banco
try:
    conn = duckdb.connect(database=db_path, read_only=False)
    st.sidebar.success(f"Conectado ao banco de dados: {db_path}")
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.stop()

# Função auxiliar
def query_dataframe(query):
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        return pd.DataFrame()

# --- Filtro por Ano-Trimestre ---
ano_trimestres = ["2024-1A", "2024-1B", "2024-2A", "2024-2B", "2025-1A", "2025-1B"]
selected_trimestre = st.selectbox("Selecione o Ano-Trimestre", ano_trimestres)

def fetch_repositories(trimestre):
    query = f"SELECT repo_name FROM repositories WHERE repo_name LIKE '%{trimestre}%'"
    return query_dataframe(query)

repositories_df = fetch_repositories(selected_trimestre)

if repositories_df.empty:
    st.error("Nenhum repositório encontrado para o trimestre selecionado.")
    st.stop()

selected_repo = st.selectbox("Selecione um repositório", repositories_df['repo_name'])

# Funções com CAST aplicados
def fetch_commits(repo_name, start_date=None, end_date=None):
    query = f"""
        SELECT sha, message, author, CAST(date AS DATE) AS date, repo_name, url 
        FROM commits 
        WHERE repo_name = '{repo_name}'
    """
    if start_date and end_date:
        query += f" AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
    return query_dataframe(query)

def fetch_pull_requests(repo_name, start_date=None, end_date=None):
    query = f"""
        SELECT number, title, author, state, CAST(created_at AS DATE) AS created_at, repo_name, url 
        FROM pull_requests 
        WHERE repo_name = '{repo_name}'
    """
    if start_date and end_date:
        query += f" AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
    return query_dataframe(query)

# Título
st.title("Painel de Dados com Streamlit - DuckDB")

# Filtro de Datas
dates_df = query_dataframe(f"""
    SELECT MIN(CAST(date AS DATE)) AS min_date, MAX(CAST(date AS DATE)) AS max_date
    FROM commits
    WHERE repo_name = '{selected_repo}'
""")

if not dates_df.empty and pd.notnull(dates_df.iloc[0]['min_date']):
    default_start_date = pd.to_datetime(dates_df.iloc[0]['min_date']).date()
    default_end_date = pd.to_datetime(dates_df.iloc[0]['max_date']).date()
    selected_dates = st.date_input("Selecione o intervalo de datas", value=(default_start_date, default_end_date))
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date, end_date = default_start_date, default_end_date
else:
    st.info("Não há informações de data para este repositório.")
    start_date, end_date = None, None

# Dados
commits_df = fetch_commits(selected_repo, start_date, end_date)
pull_requests_df = fetch_pull_requests(selected_repo, start_date, end_date)

# KPIs
st.header("KPIs")
if start_date and end_date:
    def count_commits_by_prefix(prefix):
        return query_dataframe(f"""
            SELECT COUNT(*) AS total
            FROM commits
            WHERE repo_name = '{selected_repo}'
              AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND message LIKE '{prefix}:%'
        """).iloc[0]['total']

    total_commits = query_dataframe(f"""
        SELECT COUNT(*) AS total
        FROM commits
        WHERE repo_name = '{selected_repo}'
          AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """).iloc[0]['total']

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Commits", total_commits)
    col2.metric("Commits 'feat'", count_commits_by_prefix("feat"))
    col3.metric("Commits 'fix'", count_commits_by_prefix("fix"))
    col4.metric("Commits 'docs'", count_commits_by_prefix("docs"))
else:
    st.write("Defina um intervalo de datas para ver os KPIs.")

# Exibir Commits
if not commits_df.empty:
    st.header(f"Commits no Repositório: {selected_repo}")
    commits_df['Link'] = commits_df['url'].apply(lambda x: f"[🔗 Commit]({x})")
    st.dataframe(commits_df[['sha', 'message', 'author', 'date', 'repo_name', 'Link']])
else:
    st.write("Nenhum commit encontrado.")

# Exibir Pull Requests
if not pull_requests_df.empty:
    st.header(f"Pull Requests no Repositório: {selected_repo}")
    pull_requests_df['Link'] = pull_requests_df['url'].apply(lambda x: f"[🔗 Pull Request]({x})")
    st.dataframe(pull_requests_df[['number', 'title', 'author', 'state', 'created_at', 'repo_name', 'Link']])
else:
    st.write("Nenhum pull request encontrado.")

# Gráficos
st.header("Gráficos")

# Commits por Autor
commits_by_author = query_dataframe(f"""
    SELECT author, COUNT(*) AS quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY author
    ORDER BY quantidade DESC
""")

if not commits_by_author.empty:
    fig_author = px.bar(commits_by_author, x='author', y='quantidade', title="Commits por Autor")
    st.plotly_chart(fig_author, use_container_width=True)

# Commits por Tipo
commits_by_type = query_dataframe(f"""
    SELECT 
        CASE 
            WHEN message LIKE 'feat:%' THEN 'feat'
            WHEN message LIKE 'fix:%' THEN 'fix'
            WHEN message LIKE 'docs:%' THEN 'docs'
            ELSE 'other'
        END AS commit_type,
        COUNT(*) AS quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY commit_type
""")

if not commits_by_type.empty:
    fig_type = px.pie(commits_by_type, names='commit_type', values='quantidade', title="Tipos de Commits", hole=0.6)
    st.plotly_chart(fig_type, use_container_width=True)

# Métricas de Pull Requests
st.header("Métricas de Pull Requests")
pr_counts = query_dataframe(f"""
    SELECT author, COUNT(*) AS quantidade
    FROM pull_requests
    WHERE repo_name = '{selected_repo}'
      AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY author
    ORDER BY quantidade DESC
""")
if not pr_counts.empty:
    st.subheader("Quantidade de Pull Requests por Autor")
    st.dataframe(pr_counts)
else:
    st.info("Nenhum pull request para este período.")

# Commits Diários por Contribuinte
st.header("Análise por Contribuinte")
contributors = query_dataframe(f"""
    SELECT DISTINCT author FROM commits WHERE repo_name = '{selected_repo}'
""").sort_values('author')

if not contributors.empty:
    selected_contributor = st.selectbox("Selecione um Contribuinte", contributors['author'])
    daily_commits = query_dataframe(f"""
        SELECT CAST(date AS DATE) AS day, COUNT(*) AS quantidade
        FROM commits
        WHERE repo_name = '{selected_repo}'
          AND author = '{selected_contributor}'
          AND CAST(date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY day
        ORDER BY day
    """)
    if not daily_commits.empty:
        fig_line = px.line(daily_commits, x='day', y='quantidade', title=f"Commits Diários de {selected_contributor}", markers=True)
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Nenhum commit deste usuário no período.")
else:
    st.info("Nenhum contribuinte encontrado.")

# Fechar conexão
conn.close()
