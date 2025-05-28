import streamlit as st
import pandas as pd
import duckdb
import datetime
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Análise de Commits por Repositório", page_icon="📊", layout="wide")
st.title("Análise de Commits - Repositórios sem commits na janela, com commits após a janela e detalhamento por projeto")

duckdb_path = os.path.join("duckdb_exports", "default.duckdb")

@st.cache_resource
def connect_to_duckdb():
    return duckdb.connect(duckdb_path)

conn = connect_to_duckdb()

st.sidebar.header("Filtros de Data")
default_end_date = datetime.date(2025, 5, 17)
default_start_date = datetime.date(2025, 5, 5)

start_date = st.sidebar.date_input("Data Inicial da Sprint", value=default_start_date)
end_date = st.sidebar.date_input("Data Final da Sprint", value=default_end_date)
end_time = st.sidebar.time_input("Hora limite (final da sprint)", value=datetime.time(4, 59, 59))

start_datetime = f"{start_date} 00:00:00"
end_datetime = f"{end_date} {end_time}"

# Filtro: Tipo de repositório
filtro_tipo = st.sidebar.selectbox(
    "Tipo de Repositório",
    ("Todos", "INTERNO", "PUBLICO")
)

st.info("As datas estão em GMT, considere sempre colocar 3 horas na frente a data final para São Paulo, Brasil.")
st.info(f"Analisando commits de {start_datetime} até {end_datetime}")

tab1, tab2, tab3 = st.tabs([
    "Repositórios SEM commits na janela",
    "Repositórios SEM commits na janela MAS COM commits após",
    "Detalhar commits por projeto"
])

# 1. Pegar todos os repositórios válidos (contendo -INTERNO ou -PUBLICO, conforme filtro)
if filtro_tipo == "INTERNO":
    where_clause = "repo_name ILIKE '%-INTERNO%'"
elif filtro_tipo == "PUBLICO":
    where_clause = "repo_name ILIKE '%-PUBLICO%'"
else:
    where_clause = "repo_name ILIKE '%-INTERNO%' OR repo_name ILIKE '%-PUBLICO%'"

query_repos = f"""
SELECT DISTINCT repo_name
FROM commits
WHERE {where_clause}
ORDER BY repo_name;
"""
df_repos = conn.execute(query_repos).fetchdf()
repositorios = df_repos['repo_name'].tolist()

registros_sem_janela = []
registros_sem_janela_com_apos = []

for repo in repositorios:
    # Verifica se houve commit na janela
    query_janela = f"""
    SELECT COUNT(*)
    FROM commits
    WHERE repo_name = '{repo}'
    AND CAST(date AS TIMESTAMP) >= TIMESTAMP '{start_datetime}'
    AND CAST(date AS TIMESTAMP) <= TIMESTAMP '{end_datetime}';
    """
    commits_na_janela = conn.execute(query_janela).fetchone()[0]

    # Se NÃO houve commit na janela
    if commits_na_janela == 0:
        # Pega os authors relacionados a esse repo, excluindo os indesejados
        query_authors = f"""
        SELECT DISTINCT author
        FROM commits
        WHERE repo_name = '{repo}'
        AND author NOT IN ('Inteli Hub', 'José Romualdo');
        """
        df_authors = conn.execute(query_authors).fetchdf()
        authors_list = df_authors['author'].tolist()
        authors_str = ", ".join(authors_list) if authors_list else "Nenhum author registrado"

        registros_sem_janela.append({
            'Repositório': repo,
            'Commits na Janela?': 'Não',
            'Authors': authors_str
        })

        # Verifica se houve commit APÓS a janela
        query_apos = f"""
        SELECT COUNT(*)
        FROM commits
        WHERE repo_name = '{repo}'
        AND CAST(date AS TIMESTAMP) > TIMESTAMP '{end_datetime}';
        """
        commits_apos = conn.execute(query_apos).fetchone()[0]

        if commits_apos > 0:
            # Pega os authors dos commits após a janela, excluindo os indesejados
            query_authors_apos = f"""
            SELECT DISTINCT author
            FROM commits
            WHERE repo_name = '{repo}'
            AND CAST(date AS TIMESTAMP) > TIMESTAMP '{end_datetime}'
            AND author NOT IN ('Inteli Hub', 'José Romualdo');
            """
            df_authors_apos = conn.execute(query_authors_apos).fetchdf()
            authors_list_apos = df_authors_apos['author'].tolist()
            authors_str_apos = ", ".join(authors_list_apos) if authors_list_apos else "Nenhum author registrado"

            registros_sem_janela_com_apos.append({
                'Repositório': repo,
                'Commits após a Janela': commits_apos,
                'Authors': authors_str_apos
            })

# TAB 1 - SEM commits na janela
with tab1:
    st.header("Repositórios SEM commits na janela selecionada")
    df_sem_janela = pd.DataFrame(registros_sem_janela)
    if not df_sem_janela.empty:
        st.dataframe(df_sem_janela, use_container_width=True)
        st.error(f"{len(df_sem_janela)} repositórios NÃO fizeram commits na janela.")
    else:
        st.success("Todos os repositórios realizaram commits na janela selecionada!")

# TAB 2 - SEM commits na janela MAS COM commits após
with tab2:
    st.header("Repositórios SEM commits na janela MAS COM commits após a janela")
    df_sem_janela_com_apos = pd.DataFrame(registros_sem_janela_com_apos)
    if not df_sem_janela_com_apos.empty:
        st.dataframe(df_sem_janela_com_apos, use_container_width=True)
        st.warning(f"{len(df_sem_janela_com_apos)} repositórios NÃO fizeram commits na janela mas fizeram APÓS.")
    else:
        st.success("Nenhum repositório fez commit após a janela sem ter feito na janela!")

# TAB 3 - Detalhamento por projeto
with tab3:
    st.header("Detalhamento de commits por repositório")

    if repositorios:
        selected_repo = st.selectbox("Selecione o repositório para detalhar", repositorios, key="repo_select")

        # Consulta detalhada dos commits
        query_detalhe = f"""
        SELECT author, date, message
        FROM commits
        WHERE repo_name = '{selected_repo}'
        ORDER BY CAST(date AS TIMESTAMP);
        """
        df_detalhe = conn.execute(query_detalhe).fetchdf()

        if not df_detalhe.empty:
            st.dataframe(df_detalhe, use_container_width=True)
            st.success(f"Total de {len(df_detalhe)} commits encontrados para {selected_repo} na janela selecionada.")

            # Consulta agregada: commits por dia
            query_agrupada = f"""
            SELECT 
                DATE_TRUNC('day', CAST(date AS TIMESTAMP)) AS dia,
                COUNT(*) AS total_commits
            FROM commits
            WHERE repo_name = '{selected_repo}'
            GROUP BY dia
            ORDER BY dia;
            """
            df_agrupada = conn.execute(query_agrupada).fetchdf()

            if not df_agrupada.empty:
                st.subheader("Gráfico: Commits por dia")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.bar(df_agrupada['dia'].astype(str), df_agrupada['total_commits'])
                ax.set_xlabel("Data")
                ax.set_ylabel("Número de Commits")
                ax.set_title(f"Commits por dia - {selected_repo}")
                plt.xticks(rotation=45)
                st.pyplot(fig)
            else:
                st.warning("Nenhum commit encontrado para gerar gráfico.")
        else:
            st.warning(f"Nenhum commit encontrado para {selected_repo} na janela selecionada.")
    else:
        st.info("Nenhum repositório encontrado para o filtro atual.")
