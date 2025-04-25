import streamlit as st
import duckdb
import pandas as pd

# Use cache_resource to cache the DuckDB connection
@st.cache_resource
def get_connection(db_path: str):
    """
    Cria e retorna uma conexão DuckDB para o arquivo especificado.
    Essa função é chamada apenas uma vez por aplicação.
    """
    return duckdb.connect(database=db_path, read_only=False)

# Cache apenas os resultados das queries, que são DataFrames serializáveis
@st.cache_data
def run_query(db_path: str, query: str):
    """
    Executa a query SQL usando a conexão DuckDB e retorna um DataFrame ou um erro.
    A conexão em si é obtida via get_connection, que usa cache_resource.
    """
    conn = get_connection(db_path)
    try:
        df = conn.execute(query).fetchdf()
        return df, None
    except Exception as e:
        return None, str(e)

def main():
    st.title("Interface de Consulta DuckDB")

    # Campo para caminho do banco DuckDB
    db_path = st.text_input(
        "Caminho para o arquivo DuckDB",
        value="duckdb_exports/default.duckdb"
    )

    # Área de texto para SQL
    query = st.text_area(
        "Digite sua consulta SQL aqui:",
        value="SELECT * FROM repo_name LIMIT 10",
        height=200
    )

    if st.button("Executar Consulta"):
        df, error = run_query(db_path, query)
        if error:
            st.error(f"Erro ao executar a consulta: {error}")
        else:
            st.success(f"Consulta executada com sucesso. {len(df)} registros retornados.")
            st.dataframe(df)

if __name__ == "__main__":
    main()
