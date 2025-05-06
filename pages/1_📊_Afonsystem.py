import streamlit as st
import pandas as pd
import plotly.express as px
import os
import duckdb

# Configurar o layout da página como wide
st.set_page_config(layout="wide")

# Configuração do DuckDB
db_path = os.path.join("duckdb_exports", "default.duckdb")

# Conectar ao DuckDB
try:
    conn = duckdb.connect(database=db_path, read_only=False)
    st.sidebar.success(f"Conectado ao banco de dados: {db_path}")
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.stop()

# Função auxiliar para converter o resultado da query em DataFrame
def query_dataframe(query):
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        return pd.DataFrame()

# --- Filtro por Ano-Trimestre ---
ano_trimestres = ["2024-1A", "2024-1B", "2024-2A", "2024-2B", "2025-1A", "2025-1B"]
selected_trimestre = st.selectbox("Selecione o Ano-Trimestre", ano_trimestres)

# Função para carregar os repositórios baseando-se no ano-trimestre
def fetch_repositories(trimestre):
    query = f"SELECT repo_name FROM repositories WHERE repo_name LIKE '%{trimestre}%'"
    return query_dataframe(query)

repositories_df = fetch_repositories(selected_trimestre)

# Se não houver repositórios para o trimestre, exibe mensagem de erro
if repositories_df.empty:
    st.error("Nenhum repositório encontrado para o trimestre selecionado.")
    st.stop()

# Dropdown para selecionar o repositório dentre os disponíveis
selected_repo = st.selectbox("Selecione um repositório", repositories_df['repo_name'])

# --- Funções para buscar commits e pull requests com filtros de data ---
def fetch_commits(repo_name, start_date=None, end_date=None):
    query = f"""
        SELECT sha, message, author, date, repo_name, url 
        FROM commits 
        WHERE repo_name = '{repo_name}'
    """
    if start_date and end_date:
        query += f" AND date >= '{start_date}' AND date <= '{end_date}'"
    return query_dataframe(query)

def fetch_pull_requests(repo_name, start_date=None, end_date=None):
    query = f"""
        SELECT number, title, author, state, created_at, repo_name, url 
        FROM pull_requests 
        WHERE repo_name = '{repo_name}'
    """
    if start_date and end_date:
        query += f" AND created_at >= '{start_date}' AND created_at <= '{end_date}'"
    return query_dataframe(query)

st.title("Painel de Dados com Streamlit - DuckDB")

# --- Filtro de Datas ---
dates_df = query_dataframe(f"""
    SELECT min(date) as min_date, max(date) as max_date 
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

# Carregar dados de commits e pull requests com os filtros aplicados
commits_df = fetch_commits(selected_repo, start_date, end_date)
pull_requests_df = fetch_pull_requests(selected_repo, start_date, end_date)

# --- KPIs ---
st.header("KPIs")
if start_date and end_date:
    total_commits = query_dataframe(f"""
        SELECT COUNT(*) as total 
        FROM commits 
        WHERE repo_name = '{selected_repo}'
          AND date >= '{start_date}' AND date <= '{end_date}'
    """).iloc[0]['total']
    
    feat_commits = query_dataframe(f"""
        SELECT COUNT(*) as total 
        FROM commits 
        WHERE repo_name = '{selected_repo}'
          AND date >= '{start_date}' AND date <= '{end_date}'
          AND message LIKE 'feat:%'
    """).iloc[0]['total']
    
    fix_commits = query_dataframe(f"""
        SELECT COUNT(*) as total 
        FROM commits 
        WHERE repo_name = '{selected_repo}'
          AND date >= '{start_date}' AND date <= '{end_date}'
          AND message LIKE 'fix:%'
    """).iloc[0]['total']
    
    docs_commits = query_dataframe(f"""
        SELECT COUNT(*) as total 
        FROM commits 
        WHERE repo_name = '{selected_repo}'
          AND date >= '{start_date}' AND date <= '{end_date}'
          AND message LIKE 'docs:%'
    """).iloc[0]['total']
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Commits", total_commits)
    with col2:
        st.metric("Commits 'feat'", feat_commits)
    with col3:
        st.metric("Commits 'fix'", fix_commits)
    with col4:
        st.metric("Commits 'docs'", docs_commits)
else:
    st.write("Não há filtro de data definido para os KPIs.")

# --- Exibição dos Dados ---
if not commits_df.empty:
    st.header(f"Commits no Repositório: {selected_repo}")
    commits_df['Link'] = commits_df['url'].apply(lambda x: f"[🔗 Commit]({x})")
    st.dataframe(commits_df[['sha', 'message', 'author', 'date', 'repo_name', 'Link']])
else:
    st.write("Nenhum commit encontrado para este repositório no intervalo de datas selecionado.")

if not pull_requests_df.empty:
    st.header(f"Pull Requests no Repositório: {selected_repo}")
    pull_requests_df['Link'] = pull_requests_df['url'].apply(lambda x: f"[🔗 Pull Request]({x})")
    st.dataframe(pull_requests_df[['number', 'title', 'author', 'state', 'created_at', 'repo_name', 'Link']])
else:
    st.write("Nenhum pull request encontrado para este repositório no intervalo de datas selecionado.")

# --- Gráficos ---
st.header("Gráficos")

# Gráfico de Barras: Commits por Autor
commits_by_author_query = f"""
    SELECT author, COUNT(*) as quantidade 
    FROM commits 
    WHERE repo_name = '{selected_repo}' 
      AND date >= '{start_date}' AND date <= '{end_date}'
    GROUP BY author
    ORDER BY quantidade DESC
"""
commits_by_author = query_dataframe(commits_by_author_query)

if not commits_by_author.empty:
    fig_author = px.bar(
        commits_by_author,
        x='author',
        y='quantidade',
        title="Commits por Autor",
        labels={'author': 'Autor', 'quantidade': 'Número de Commits'}
    )
    st.plotly_chart(fig_author, use_container_width=True)
else:
    st.info("Não há dados suficientes para o gráfico de commits por autor.")

# Gráfico de Pizza: Distribuição de Commits por Tipo (feat, fix, docs, other)
commits_by_type_query = f"""
    SELECT 
        CASE
            WHEN message LIKE 'feat:%' THEN 'feat'
            WHEN message LIKE 'fix:%' THEN 'fix'
            WHEN message LIKE 'docs:%' THEN 'docs'
            ELSE 'other'
        END as commit_type,
        COUNT(*) as quantidade
    FROM commits
    WHERE repo_name = '{selected_repo}'
      AND date >= '{start_date}' AND date <= '{end_date}'
    GROUP BY commit_type
"""
commits_by_type = query_dataframe(commits_by_type_query)

if not commits_by_type.empty:
    fig_type = px.pie(
        commits_by_type,
        names='commit_type',
        values='quantidade',
        title="Distribuição de Commits por Tipo",
        hole=0.6
    )
    st.plotly_chart(fig_type, use_container_width=True)
else:
    st.info("Não há dados suficientes para o gráfico de distribuição de commits.")

# --- Métricas de Pull Requests ---
st.header("Métricas de Pull Requests")
if start_date and end_date:
    pr_counts_query = f"""
        SELECT author, COUNT(*) as quantidade 
        FROM pull_requests
        WHERE repo_name = '{selected_repo}'
          AND created_at >= '{start_date}' AND created_at <= '{end_date}'
        GROUP BY author
        ORDER BY quantidade DESC
    """
    pr_counts = query_dataframe(pr_counts_query)
    
    if not pr_counts.empty:
        st.subheader("Quantidade de Pull Requests por Autor")
        st.dataframe(pr_counts)
    else:
        st.info("Não há pull requests para os filtros selecionados.")
else:
    st.info("Não há filtro de data definido para as métricas de pull requests.")

# --- Análise por Contribuinte: Gráfico de Commits Diários ---
st.header("Análise por Contribuinte")
if start_date and end_date:
    # Obter lista de contribuidores para o repositório
    contributors = query_dataframe(f"SELECT DISTINCT author FROM commits WHERE repo_name = '{selected_repo}'").sort_values('author')
    
    if not contributors.empty:
        selected_contributor = st.selectbox("Selecione um Contribuinte", contributors['author'])
        
        daily_commits_query = f"""
            SELECT CAST(date AS DATE) as day, COUNT(*) as quantidade 
            FROM commits 
            WHERE repo_name = '{selected_repo}' 
              AND author = '{selected_contributor}' 
              AND date >= '{start_date}' AND date <= '{end_date}'
            GROUP BY day
            ORDER BY day
        """
        daily_commits = query_dataframe(daily_commits_query)
        
        if not daily_commits.empty:
            fig_line = px.line(
                daily_commits, 
                x='day', 
                y='quantidade', 
                title=f"Commits Diários de {selected_contributor}",
                markers=True
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Nenhum commit encontrado para este contribuinte no intervalo de datas selecionado.")
    else:
        st.info("Não foram encontrados contribuidores para este repositório.")
else:
    st.info("Não há filtro de data definido para a análise por contribuinte.")

# Fechar a conexão com o DuckDB ao finalizar
conn.close()