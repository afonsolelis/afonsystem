import streamlit as st
import pandas as pd
import duckdb
import datetime
import matplotlib.pyplot as plt
import os

# Configuração da página
st.set_page_config(page_title="Análise de Commits", page_icon="📊", layout="wide")
st.title("Análise de Commits por Sprint")

# Caminho para o banco DuckDB
duckdb_path = os.path.join("duckdb_exports", "default.duckdb")

# Função para conectar ao DuckDB
@st.cache_resource
def connect_to_duckdb():
    return duckdb.connect(duckdb_path)

# Conectar
conn = connect_to_duckdb()

# Interface do usuário para filtros de data
st.sidebar.header("Filtros de Data")

# Datas padrão
default_end_date = datetime.date(2025, 3, 15)
default_start_date = datetime.date(2025, 3, 5)

# Seleção de datas
start_date = st.sidebar.date_input("Data Inicial da Sprint", value=default_start_date)
end_date = st.sidebar.date_input("Data Final da Sprint", value=default_end_date)

# Hora limite
end_time = st.sidebar.time_input("Hora limite (final da sprint)", value=datetime.time(4, 59, 59))

# Formatação das datas
start_datetime = f"{start_date} 00:00:00"
end_datetime = f"{end_date} {end_time}"

# Botão de análise
if st.sidebar.button("Executar Análise"):
    try:
        st.info("As datas estão em GMT, considere sempre colocar 3 horas na frente a data final para São Paulo, Brasil.")
        st.info(f"Analisando commits de {start_datetime} até {end_datetime}")

        tab1, tab2, tab3 = st.tabs([
            "Commits Dentro do Prazo", 
            "Alunos Sem Commits na Sprint", 
            "Commits Após o Prazo"
        ])

        # --- TAB 1 ---
        with tab1:
            st.header("Autores com Commits Dentro do Prazo")

            query1 = f"""
            SELECT 
                t1.repo_name,
                t1.author,
                t1.date AS commit_date,
                t1.message
            FROM commits t1
            JOIN (
                SELECT 
                    repo_name, 
                    MAX(date) AS max_date
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND date >= '{start_datetime}'
                AND date <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY repo_name
            ) t2 ON t1.repo_name = t2.repo_name AND t1.date = t2.max_date
            WHERE (t1.repo_name ILIKE '%INTERNO%' OR t1.repo_name ILIKE '%PUBLICO%')
            AND t1.date >= '{start_datetime}' AND t1.date <= '{end_datetime}'
            AND t1.author NOT IN ('Inteli Hub', 'José Romualdo')
            ORDER BY t1.repo_name
            """
            df1 = conn.execute(query1).fetchdf()

            if not df1.empty:
                df1['commit_date'] = pd.to_datetime(df1['commit_date']).dt.tz_localize(None).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
                df1['commit_date'] = df1['commit_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                st.dataframe(df1, use_container_width=True)
                st.success(f"Total de {len(df1)} commits dentro do prazo")

                # Top repositórios
                query_repos = f"""
                SELECT repo_name, COUNT(*) as num_commits
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND date >= '{start_datetime}' AND date <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY repo_name
                ORDER BY num_commits DESC
                LIMIT 10
                """
                df_repos = conn.execute(query_repos).fetchdf()

                # Top autores
                query_authors = f"""
                SELECT author, COUNT(*) as num_commits
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND date >= '{start_datetime}' AND date <= '{end_datetime}'
                AND author NOT IN ('Inteli Hub', 'José Romualdo')
                GROUP BY author
                ORDER BY num_commits DESC
                LIMIT 10
                """
                df_authors = conn.execute(query_authors).fetchdf()

                st.subheader("Análise de Commits na Sprint")
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("Top 10 Repositórios com Mais Commits")
                    if not df_repos.empty:
                        st.dataframe(df_repos)
                        fig, ax = plt.subplots(figsize=(10, 6))
                        ax.barh(df_repos['repo_name'], df_repos['num_commits'])
                        ax.set_xlabel('Número de Commits')
                        ax.set_ylabel('Repositório')
                        ax.set_title('Top 10 Repositórios com Mais Commits')
                        st.pyplot(fig)

                with col2:
                    st.subheader("Top 10 Autores Mais Ativos")
                    if not df_authors.empty:
                        st.dataframe(df_authors)
                        fig, ax = plt.subplots(figsize=(10, 6))
                        ax.barh(df_authors['author'], df_authors['num_commits'])
                        ax.set_xlabel('Número de Commits')
                        ax.set_ylabel('Autor')
                        ax.set_title('Top 10 Autores com Mais Commits')
                        st.pyplot(fig)

            else:
                st.warning("Nenhum resultado encontrado para commits dentro do prazo.")

        # --- TAB 2 ---
        with tab2:
            st.header("Alunos Sem Commits na Sprint")
            query2 = f"""
            SELECT DISTINCT author
            FROM commits
            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
            AND author NOT IN ('Inteli Hub', 'José Romualdo')
            AND date >= '{end_datetime}'
            AND author NOT IN (
                SELECT DISTINCT author
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND date >= '{start_datetime}' AND date < '{end_datetime}'
            )
            ORDER BY author
            """
            df2 = conn.execute(query2).fetchdf()

            if not df2.empty:
                st.dataframe(df2, use_container_width=True)
                st.error(f"Total de {len(df2)} alunos sem commits na sprint")
            else:
                st.success("Todos os alunos fizeram commits durante a sprint!")

        # --- TAB 3 ---
        with tab3:
            st.header("Repositórios com Commits Após o Prazo e Seus Colaboradores")

            query3_commits = f"""
            SELECT 
                author,
                repo_name,
                date AS commit_date,
                message
            FROM commits
            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
            AND author NOT IN ('Inteli Hub', 'José Romualdo')
            AND date >= '{end_datetime}'
            AND author IN (
                SELECT DISTINCT author
                FROM commits
                WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                AND date >= '{end_datetime}'
                AND author NOT IN (
                    SELECT DISTINCT author
                    FROM commits
                    WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                    AND date >= '{start_datetime}' AND date < '{end_datetime}'
                )
            )
            ORDER BY date DESC
            """
            df_commits = conn.execute(query3_commits).fetchdf()

            if not df_commits.empty:
                df_commits['commit_date'] = pd.to_datetime(df_commits['commit_date']).dt.tz_localize(None).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
                df_commits['commit_date'] = df_commits['commit_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                st.subheader("Lista de todos os commits realizados após o prazo")
                st.dataframe(df_commits, use_container_width=True)

                query3 = f"""
                WITH repos AS (
                    SELECT DISTINCT repo_name
                    FROM commits
                    WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                    AND author NOT IN ('Inteli Hub', 'José Romualdo')
                    AND date >= '{end_datetime}'
                    AND author IN (
                        SELECT DISTINCT author
                        FROM commits
                        WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                        AND date >= '{end_datetime}'
                        AND author NOT IN (
                            SELECT DISTINCT author
                            FROM commits
                            WHERE (repo_name ILIKE '%INTERNO%' OR repo_name ILIKE '%PUBLICO%')
                            AND date >= '{start_datetime}' AND date < '{end_datetime}'
                        )
                    )
                )
                SELECT DISTINCT c.repo_name, c.author
                FROM commits c
                JOIN repos r ON c.repo_name = r.repo_name
                WHERE c.author NOT IN ('Inteli Hub', 'José Romualdo')
                ORDER BY c.repo_name, c.author
                """
                df3 = conn.execute(query3).fetchdf()

                if not df3.empty:
                    df3 = df3.rename(columns={'repo_name': 'Repositório', 'author': 'Autor'})
                    st.subheader("Lista completa de colaboradores por repositório")
                    st.dataframe(df3, use_container_width=True)

                    repos = df3['Repositório'].unique()
                    for repo in repos:
                        authors = df3[df3['Repositório'] == repo]['Autor'].tolist()
                        st.markdown(f"### {repo}")
                        st.markdown(f"**Total de {len(authors)} colaboradores**")
                        st.markdown(f"**Colaboradores:** {', '.join(authors)}")
                        st.markdown("---")
                    
                    st.warning(f"Total de {len(repos)} repositórios que tiveram commits após o prazo")
                else:
                    st.warning("Não foi possível obter a lista de colaboradores por repositório.")
            else:
                st.success("Nenhum repositório tem commits após o prazo da sprint!")

    except Exception as e:
        st.error(f"Erro ao executar a análise: {str(e)}")

else:
    st.info("Selecione o período da sprint no menu lateral e clique em 'Executar Análise' para visualizar os resultados.")
