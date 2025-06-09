import streamlit as st
import pandas as pd
import plotly.express as px
import clickhouse_connect
from datetime import datetime

# Layout
st.set_page_config(layout="wide")

# Conectar ao ClickHouse
try:
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='afonsystem'
    )
    st.sidebar.success("✅ Conectado ao ClickHouse")
except Exception as e:
    st.error(f"Erro ao conectar ao ClickHouse: {e}")
    st.stop()

# Função auxiliar para query
def query_dataframe(query):
    try:
        result = client.query(query)
        return pd.DataFrame(result.result_rows, columns=result.column_names)
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        return pd.DataFrame()

# Cache dos autores
@st.cache_data
def get_authors(repo, start, end):
    df = query_dataframe(f"""
        SELECT DISTINCT author FROM commits
        WHERE repo_name = '{repo}'
          AND toDate(date) BETWEEN toDate('{start}') AND toDate('{end}')
        ORDER BY author
    """)
    return df['author'].tolist()

# --- Filtro por Ano-Trimestre ---
ano_trimestres = ["2024-1A", "2024-1B", "2024-2A", "2024-2B", "2025-1A", "2025-1B"]
selected_trimestre = st.selectbox("Selecione o Ano-Trimestre", ano_trimestres)

# Buscar repositórios a partir dos commits
repositories_df = query_dataframe(f"""
    SELECT DISTINCT repo_name FROM commits
    WHERE repo_name LIKE '%{selected_trimestre}%'
""")

if repositories_df.empty:
    st.error("Nenhum repositório encontrado para o trimestre selecionado.")
    st.stop()

selected_repo = st.selectbox("Selecione um repositório", repositories_df['repo_name'])

# Intervalo de datas
dates_df = query_dataframe(f"""
    SELECT MIN(toDate(date)) AS min_date, MAX(toDate(date)) AS max_date
    FROM commits
    WHERE repo_name = '{selected_repo}'
""")

if not dates_df.empty and pd.notnull(dates_df.iloc[0]['min_date']):
    default_start_date = dates_df.iloc[0]['min_date']
    default_end_date = dates_df.iloc[0]['max_date']
    selected_dates = st.date_input("Selecione o intervalo de datas", value=(default_start_date, default_end_date))
    start_date, end_date = selected_dates
else:
    st.info("Não há informações de data para este repositório.")
    start_date, end_date = None, None

# Buscar dados
def fetch_commits():
    return query_dataframe(f"""
        SELECT sha, message, author, toDate(date) AS date, repo_name, url
        FROM commits
        WHERE repo_name = '{selected_repo}'
          AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    """)

def fetch_pull_requests():
    return query_dataframe(f"""
        SELECT number, title, author, state, toDate(created_at) AS created_at, repo_name, url
        FROM pull_requests
        WHERE repo_name = '{selected_repo}'
          AND toDate(created_at) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    """)

commits_df = fetch_commits()
pull_requests_df = fetch_pull_requests()

# KPIs
st.header("KPIs")

def count_commits_by_type(type_prefix):
    df = query_dataframe(f"""
        SELECT COUNT(*) AS total
        FROM commits
        WHERE repo_name = '{selected_repo}'
          AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
          AND lower(message) LIKE '{type_prefix.lower()}%'
    """)
    return df.iloc[0]['total'] if not df.empty else 0

total_commits = count_commits_by_type("")
feat_commits = count_commits_by_type('feat')
fix_commits = count_commits_by_type('fix')
docs_commits = count_commits_by_type('docs')
chore_commits = count_commits_by_type('chore')
refactor_commits = count_commits_by_type('refactor')
test_commits = count_commits_by_type('test')

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Commits", total_commits)
col2.metric("'feat'", feat_commits)
col3.metric("'fix'", fix_commits)
col4.metric("'docs'", docs_commits)

col5, col6, col7 = st.columns(3)
col5.metric("'chore'", chore_commits)
col6.metric("'refactor'", refactor_commits)
col7.metric("'test'", test_commits)

# Tabela de Commits
st.header(f"Commits no Repositório: {selected_repo}")
if not commits_df.empty:
    commits_df['Link'] = commits_df['url'].apply(lambda x: f"[🔗 Commit]({x})")
    st.dataframe(commits_df[['sha', 'message', 'author', 'date', 'repo_name', 'Link']])
else:
    st.write("Nenhum commit encontrado.")

# Tabela de Pull Requests
st.header(f"Pull Requests no Repositório: {selected_repo}")
if not pull_requests_df.empty:
    pull_requests_df['Link'] = pull_requests_df['url'].apply(lambda x: f"[🔗 Pull Request]({x})")
    st.dataframe(pull_requests_df[['number', 'title', 'author', 'state', 'created_at', 'repo_name', 'Link']])
else:
    st.write("Nenhum pull request encontrado.")

# Gráficos
st.header("Gráficos")

# Commits por Autor
authors_df = query_dataframe(f"""
    SELECT author, COUNT(*) AS quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    GROUP BY author
    ORDER BY quantidade DESC
""")

if not authors_df.empty:
    fig_author = px.bar(authors_df, x='author', y='quantidade', title="Commits por Autor")
    st.plotly_chart(fig_author, use_container_width=True)

# Commits por Tipo
commits_by_type = query_dataframe(f"""
    SELECT 
        CASE 
            WHEN lower(message) LIKE 'feat%' THEN 'feat'
            WHEN lower(message) LIKE 'fix%' THEN 'fix'
            WHEN lower(message) LIKE 'docs%' THEN 'docs'
            WHEN lower(message) LIKE 'chore%' THEN 'chore'
            WHEN lower(message) LIKE 'refactor%' THEN 'refactor'
            WHEN lower(message) LIKE 'merge%' THEN 'merge'
            WHEN lower(message) LIKE 'test%' THEN 'test'
            ELSE 'other'
        END AS commit_type,
        COUNT(*) AS quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    GROUP BY commit_type
""")

if not commits_by_type.empty:
    fig_type = px.pie(commits_by_type, names='commit_type', values='quantidade', title="Tipos de Commits", hole=0.6)
    st.plotly_chart(fig_type, use_container_width=True)

# Commits por Tipo e Autor
commits_by_type_author = query_dataframe(f"""
    SELECT 
        author,
        CASE 
            WHEN lower(message) LIKE 'feat%' THEN 'feat'
            WHEN lower(message) LIKE 'fix%' THEN 'fix'
            WHEN lower(message) LIKE 'docs%' THEN 'docs'
            WHEN lower(message) LIKE 'chore%' THEN 'chore'
            WHEN lower(message) LIKE 'refactor%' THEN 'refactor'
            WHEN lower(message) LIKE 'merge%' THEN 'merge'
            WHEN lower(message) LIKE 'test%' THEN 'test'
            ELSE 'other'
        END AS commit_type,
        COUNT(*) AS quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    GROUP BY author, commit_type
""")

if not commits_by_type_author.empty:
    fig_type_author = px.bar(commits_by_type_author, x='author', y='quantidade', color='commit_type',
                             barmode='group', title="Commits por Tipo e Autor")
    st.plotly_chart(fig_type_author, use_container_width=True)

# Commits Diários por Autor
st.header("Commits Diários por Autor")
authors_list = get_authors(selected_repo, start_date, end_date)
selected_author = st.selectbox("Selecione um autor", authors_list, key="author_selector")

commits_by_day = query_dataframe(f"""
    SELECT toDate(date) AS dia, COUNT(*) AS total
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND author = '{selected_author}'
      AND toDate(date) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
    GROUP BY dia
    ORDER BY dia
""")

if not commits_by_day.empty:
    fig_daily = px.line(commits_by_day, x='dia', y='total', markers=True,
                        title=f"Commits Diários de {selected_author}")
    st.plotly_chart(fig_daily, use_container_width=True)
else:
    st.info("Nenhum commit deste autor no período selecionado.")
