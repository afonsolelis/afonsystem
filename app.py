import streamlit as st
import pandas as pd
import plotly.express as px
import time
from datetime import datetime, date
import re
from helpers.data_collector import GitHubDataCollector
from helpers.supabase_helper import SupabaseHelper
from dotenv import load_dotenv

print(f"[DEBUG] {time.time():.2f} - Starting app initialization")
load_dotenv()
print(f"[DEBUG] {time.time():.2f} - Environment loaded")

# Layout
st.set_page_config(layout="wide")
print(f"[DEBUG] {time.time():.2f} - Streamlit config set")

# Initialize data collector with lazy loading
@st.cache_resource
def get_data_collector():
    print(f"[DEBUG] {time.time():.2f} - Creating GitHubDataCollector")
    start_time = time.time()
    collector = GitHubDataCollector()
    print(f"[DEBUG] {time.time():.2f} - GitHubDataCollector created in {time.time() - start_time:.2f}s")
    return collector

# Initialize Supabase helper with lazy loading
@st.cache_resource  
def get_supabase_helper():
    print(f"[DEBUG] {time.time():.2f} - Creating SupabaseHelper")
    start_time = time.time()
    try:
        helper = SupabaseHelper()
        print(f"[DEBUG] {time.time():.2f} - SupabaseHelper created in {time.time() - start_time:.2f}s")
        return helper
    except Exception as e:
        print(f"[DEBUG] {time.time():.2f} - SupabaseHelper failed in {time.time() - start_time:.2f}s")
        st.error(f"Failed to initialize Supabase: {e}")
        st.info("💡 Make sure your .env file has SUPABASE_URL and SUPABASE_ANON_KEY configured")
        return None

# Use cached resources directly
print(f"[DEBUG] {time.time():.2f} - Getting collector")
collector = get_data_collector()
print(f"[DEBUG] {time.time():.2f} - Getting supabase helper")
supabase_helper = get_supabase_helper()
print(f"[DEBUG] {time.time():.2f} - Both helpers initialized")

st.title("📊 Análise de Repositórios GitHub")

# Filtro de trimestre - modo simplificado para Supabase
available_quarters = ["2025-1B"]
selected_quarter = st.selectbox("Selecionar Trimestre", available_quarters)

# Obter repositórios da configuração do ambiente com cache
@st.cache_data
def get_available_repos():
    return collector.get_available_repos()

env_repos = get_available_repos()
if env_repos:
    available_repos = env_repos
else:
    st.error("Nenhum repositório configurado. Configure REPO_NAMES no arquivo .env.")
    st.code("REPO_NAMES=owner/repo1,owner/repo2")
    st.stop()

selected_repo = st.selectbox("Selecionar Repositório", available_repos)

# Botão para criar snapshot - modo simplificado para Supabase
col1, col2 = st.columns(2)

with col1:
    if st.button("🚀 Criar Snapshot", type="primary"):
        if supabase_helper:
            with st.spinner(f"Criando snapshot para {selected_repo}..."):
                # Criar container de progresso
                progress_container = st.empty()
                
                def progress_callback(message: str):
                    progress_container.info(message)
                
                try:
                    # Usar método collect_and_create_snapshot com trimestre selecionado
                    snapshot_id = collector.collect_and_create_snapshot(selected_repo, progress_callback, selected_quarter)
                    
                    if snapshot_id:
                        st.success(f"🚀 Snapshot criado com sucesso!")
                        st.info(f"🆔 ID do Snapshot: {snapshot_id}")
                        st.rerun()
                    else:
                        st.error("❌ Falha ao criar snapshot")
                        
                except Exception as e:
                    st.error(f"❌ Erro ao criar snapshot: {e}")
        else:
            st.error("❌ Supabase não configurado corretamente")

with col2:
    st.empty()  # Espaço vazio no lugar do aviso

# Seleção de snapshots para o repositório selecionado
if supabase_helper:
    st.subheader("📸 Snapshots do Repositório")
    
    # Função para obter snapshots
    def get_parquet_snapshots(repo_name, quarter):
        try:
            print(f"[DEBUG] {time.time():.2f} - Carregando snapshots para {repo_name} no trimestre {quarter}")
            snapshots = supabase_helper.list_parquet_snapshots(repo_name=repo_name, quarter=quarter)
            print(f"[DEBUG] {time.time():.2f} - Encontrados {len(snapshots)} snapshots para {repo_name}")
            return snapshots
        except Exception as e:
            print(f"[DEBUG] {time.time():.2f} - Erro ao carregar snapshots: {e}")
            st.error(f"Erro ao carregar snapshots: {e}")
            return []
    
    try:
        with st.spinner("Carregando snapshots..."):
            parquet_snapshots = get_parquet_snapshots(selected_repo, selected_quarter)
            
        if parquet_snapshots:
            # Preparar opções para o dropdown
            snapshot_options = []
            snapshot_dict = {}
            
            for snapshot in parquet_snapshots:
                snapshot_id = snapshot.get('snapshot_id', 'Desconhecido')
                timestamp = snapshot.get('timestamp', 'Desconhecido')
                commits_count = snapshot.get('commits_count', 0)
                prs_count = snapshot.get('pull_requests_count', 0)
                
                display_name = f"{timestamp} - {commits_count} commits, {prs_count} PRs"
                snapshot_options.append(display_name)
                snapshot_dict[display_name] = snapshot
            
            # Dropdown para selecionar snapshot
            selected_snapshot_display = st.selectbox(
                "Selecionar Snapshot para Análise",
                ["Selecione um snapshot..."] + snapshot_options
            )
            
            if selected_snapshot_display != "Selecione um snapshot...":
                selected_snapshot = snapshot_dict[selected_snapshot_display]
                snapshot_id = selected_snapshot.get('snapshot_id', '')
                
                # Carregar dados automaticamente se ainda não foram carregados
                commits_key = f"snapshot_commits_{snapshot_id}"
                prs_key = f"snapshot_prs_{snapshot_id}"
                
                if commits_key not in st.session_state:
                    with st.spinner("Carregando dados automaticamente..."):
                        try:
                            # Carregar dados de commits do snapshot
                            commits_df = supabase_helper.load_snapshot_data(snapshot_id, 'commits', selected_quarter)
                            if commits_df is not None:
                                st.session_state[commits_key] = commits_df
                                st.session_state["current_snapshot_id"] = snapshot_id
                            
                            # Carregar dados de PRs se disponível
                            prs_df = supabase_helper.load_snapshot_data(snapshot_id, 'pull_requests', selected_quarter)
                            if prs_df is not None:
                                st.session_state[prs_key] = prs_df
                            
                            st.success("✅ Dados carregados automaticamente!")
                            
                        except Exception as e:
                            st.error(f"Erro ao carregar dados: {e}")
                
                # Mostrar métricas
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("📝 Commits", selected_snapshot.get('commits_count', 0))
                with col2:
                    st.metric("🔀 Pull Requests", selected_snapshot.get('pull_requests_count', 0))
                with col3:
                    st.metric("📅 Data", selected_snapshot.get('timestamp', 'N/A'))
            
        else:
            st.info(f"Nenhum snapshot encontrado para {selected_repo}")
            st.write("💡 Crie seu primeiro snapshot usando o botão acima!")
            
    except Exception as e:
        st.error(f"Erro ao carregar snapshots: {e}")

st.divider()

# Análise completa dos dados dos snapshots
if supabase_helper:
    st.subheader("📊 Análise dos Dados")
    
    # Verificar se há dados carregados
    current_snapshot_id = st.session_state.get("current_snapshot_id")
    if current_snapshot_id:
        commits_key = f"snapshot_commits_{current_snapshot_id}"
        
        if commits_key in st.session_state:
            commits_df = st.session_state[commits_key].copy()
            
            # Preparar dados
            if 'date' in commits_df.columns:
                commits_df['date'] = pd.to_datetime(commits_df['date'])
                commits_df['date_only'] = commits_df['date'].dt.date
            
            # Função para extrair tipo de commit
            def extract_commit_type(message):
                if pd.isna(message):
                    return 'other'
                message = str(message).lower()
                if message.startswith('feat'):
                    return 'feat'
                elif message.startswith('fix'):
                    return 'fix'
                elif message.startswith('docs'):
                    return 'docs'
                elif message.startswith('style'):
                    return 'style'
                elif message.startswith('refactor'):
                    return 'refactor'
                elif message.startswith('test'):
                    return 'test'
                elif message.startswith('chore'):
                    return 'chore'
                else:
                    return 'other'
            
            # Adicionar tipo de commit
            if 'message' in commits_df.columns:
                commits_df['commit_type'] = commits_df['message'].apply(extract_commit_type)
            
            # Filtro de datas para sprint
            st.subheader("🗓️ Filtro de Sprint")
            if 'date' in commits_df.columns and len(commits_df) > 0:
                min_date = commits_df['date'].min().date()
                max_date = commits_df['date'].max().date()
                
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Data de Início da Sprint",
                        value=min_date,
                        min_value=min_date,
                        max_value=max_date
                    )
                with col2:
                    end_date = st.date_input(
                        "Data de Fim da Sprint",
                        value=max_date,
                        min_value=min_date,
                        max_value=max_date
                    )
                
                # Filtrar dados pela sprint
                filtered_commits = commits_df[
                    (commits_df['date_only'] >= start_date) & 
                    (commits_df['date_only'] <= end_date)
                ].copy()
            else:
                st.warning("Não foi possível detectar datas nos commits")
                filtered_commits = commits_df.copy()
                start_date = end_date = None
            
            if len(filtered_commits) > 0:
                # KPIs de tipos de commits
                st.subheader("📈 KPIs por Tipo de Commit")
                if 'commit_type' in filtered_commits.columns:
                    commit_counts = filtered_commits['commit_type'].value_counts()
                    
                    # Mapear nomes em português
                    type_names = {
                        'feat': 'Features',
                        'fix': 'Correções',
                        'docs': 'Documentação',
                        'style': 'Estilo',
                        'refactor': 'Refatoração',
                        'test': 'Testes',
                        'chore': 'Tarefas',
                        'other': 'Outros'
                    }
                    
                    # Mostrar KPIs em colunas
                    cols = st.columns(4)
                    for i, (commit_type, count) in enumerate(commit_counts.head(8).items()):
                        with cols[i % 4]:
                            display_name = type_names.get(commit_type, commit_type.title())
                            st.metric(display_name, count)
                
                # Gráfico de pizza dos tipos de commits
                st.subheader("🥧 Distribuição de Tipos de Commits")
                if 'commit_type' in filtered_commits.columns and len(filtered_commits) > 0:
                    commit_counts = filtered_commits['commit_type'].value_counts()
                    
                    # Preparar dados para o gráfico
                    pie_data = pd.DataFrame({
                        'Tipo': [type_names.get(t, t.title()) for t in commit_counts.index],
                        'Quantidade': commit_counts.values
                    })
                    
                    fig_pie = px.pie(
                        pie_data, 
                        values='Quantidade', 
                        names='Tipo',
                        title="Distribuição dos Tipos de Commits"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                # Gráfico de linha de commits por dia
                st.subheader("📈 Commits por Dia")
                if 'date_only' in filtered_commits.columns:
                    daily_commits = filtered_commits.groupby('date_only').size().reset_index()
                    daily_commits.columns = ['Data', 'Commits']
                    
                    fig_line = px.line(
                        daily_commits,
                        x='Data',
                        y='Commits',
                        title="Número de Commits por Dia",
                        markers=True
                    )
                    fig_line.update_layout(
                        xaxis_title="Data",
                        yaxis_title="Número de Commits"
                    )
                    st.plotly_chart(fig_line, use_container_width=True)
                
                # Análise individual por aluno
                st.subheader("👥 Análise Individual por Aluno")
                if 'author' in filtered_commits.columns:
                    authors = sorted(filtered_commits['author'].unique())
                    selected_author = st.selectbox(
                        "Selecionar Aluno",
                        ["Selecione um aluno..."] + authors
                    )
                    
                    if selected_author != "Selecione um aluno...":
                        author_commits = filtered_commits[
                            filtered_commits['author'] == selected_author
                        ].copy()
                        
                        if len(author_commits) > 0:
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.metric("Total de Commits do Aluno", len(author_commits))
                                
                                # Tipos de commits do aluno
                                if 'commit_type' in author_commits.columns:
                                    author_types = author_commits['commit_type'].value_counts()
                                    st.write("**Tipos de commits:**")
                                    for commit_type, count in author_types.items():
                                        display_name = type_names.get(commit_type, commit_type.title())
                                        st.write(f"• {display_name}: {count}")
                            
                            with col2:
                                # Gráfico de commits por dia do aluno
                                if 'date_only' in author_commits.columns:
                                    author_daily = author_commits.groupby('date_only').size().reset_index()
                                    author_daily.columns = ['Data', 'Commits']
                                    
                                    fig_author = px.bar(
                                        author_daily,
                                        x='Data',
                                        y='Commits',
                                        title=f"Commits por Dia - {selected_author}"
                                    )
                                    st.plotly_chart(fig_author, use_container_width=True)
                
            else:
                st.warning(f"Nenhum commit encontrado no período selecionado ({start_date} a {end_date})")
            
            # Tabela de commits (últimos registros)
            st.subheader("📝 Últimos Commits")
            display_commits = filtered_commits[['date', 'author', 'message']].head(20) if len(filtered_commits) > 0 else pd.DataFrame()
            if not display_commits.empty:
                st.dataframe(display_commits, use_container_width=True)
    else:
        st.info("💡 Selecione um snapshot acima para visualizar análises e gráficos.")
        st.write("Após selecionar um snapshot, você poderá ver:")
        st.write("• 📈 KPIs por tipo de commit")
        st.write("• 🥧 Gráfico de pizza dos tipos de commits")
        st.write("• 📈 Gráfico de linha de commits por dia")
        st.write("• 🗓️ Filtro por período de sprint")
        st.write("• 👥 Análise individual por aluno")
else:
    st.error("❌ Supabase não configurado. Verifique seu arquivo .env.")