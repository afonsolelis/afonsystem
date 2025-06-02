import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

# Configuração da página
st.set_page_config(page_title="DuckDB Query App", layout="wide")

# Título do aplicativo
st.title("Consulta SQL para DuckDB")

# Caminho para o banco de dados DuckDB
db_path = "duckdb_exports/default.duckdb"

# Verificar se o arquivo existe
if not Path(db_path).exists():
    st.error(f"O arquivo de banco de dados não foi encontrado em: {db_path}")
    st.info("Certifique-se de que o arquivo existe antes de continuar.")
    st.stop()

# Função para conectar ao DuckDB
@st.cache_resource
def get_connection():
    try:
        # Conectar ao arquivo DuckDB especificado
        conn = duckdb.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para executar a consulta
def execute_query(query):
    conn = get_connection()
    if conn:
        try:
            # Executar a consulta e retornar como DataFrame
            result = conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Erro ao executar a consulta: {e}")
            return None
    return None

# Área para digitar a consulta SQL
query = st.text_area(
    "Digite sua consulta SQL:",
    height=150,
    placeholder="SELECT * FROM sua_tabela LIMIT 10;"
)

# Botão para executar a consulta
if st.button("Executar Consulta"):
    if query:
        # Mostrar um spinner enquanto a consulta é executada
        with st.spinner("Executando consulta..."):
            # Executar a consulta
            result = execute_query(query)
            
            # Mostrar os resultados
            if result is not None:
                st.success("Consulta executada com sucesso!")
                st.dataframe(result)
                
                # Opção para baixar os resultados como CSV
                csv = result.to_csv(index=False)
                st.download_button(
                    label="Baixar resultados como CSV",
                    data=csv,
                    file_name="resultados_consulta.csv",
                    mime="text/csv"
                )
    else:
        st.warning("Por favor, digite uma consulta SQL.")

# Sidebar com informações sobre o banco de dados
with st.sidebar:
    st.header("Informações do Banco de Dados")
    conn = get_connection()
    
    if conn:
        try:
            # Listar as tabelas disponíveis no banco de dados
            tables = conn.execute("SHOW TABLES").fetchdf()
            st.subheader("Tabelas disponíveis:")
            st.dataframe(tables)
            
            # Opção para visualizar o esquema de uma tabela selecionada
            if not tables.empty:
                table_names = tables.iloc[:, 0].tolist()
                selected_table = st.selectbox("Selecione uma tabela para ver o esquema:", table_names)
                
                if selected_table:
                    schema = conn.execute(f"DESCRIBE {selected_table}").fetchdf()
                    st.subheader(f"Esquema da tabela '{selected_table}':")
                    st.dataframe(schema)
        except Exception as e:
            st.error(f"Erro ao obter informações do banco de dados: {e}")
