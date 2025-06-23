import streamlit as st
import pandas as pd
import plotly.express as px
import time
from datetime import datetime, date
import re
from helpers.data_collector import GitHubDataCollector
from helpers.supabase_helper import SupabaseHelper
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(layout="wide")

@st.cache_resource
def get_data_collector():
    collector = GitHubDataCollector()
    return collector

@st.cache_resource  
def get_supabase_helper():
    try:
        helper = SupabaseHelper()
        return helper
    except Exception as e:
        st.error(f"Failed to initialize Supabase: {e}")
        st.info("💡 Make sure your .env file has SUPABASE_URL and SUPABASE_ANON_KEY configured")
        return None

collector = get_data_collector()
supabase_helper = get_supabase_helper()

st.title("📊 Análise de Repositórios GitHub")

available_quarters = ["2025-1B"]
selected_quarter = st.selectbox("Selecionar Trimestre", available_quarters)

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

col1, col2 = st.columns(2)

with col1:
    if st.button("🚀 Criar Snapshot", type="primary"):
        if supabase_helper:
            with st.spinner(f"Criando snapshot para {selected_repo}..."):
                progress_container = st.empty()
                progress_container = st.empty()
                
                def progress_callback(message: str):
                    progress_container.info(message)
                
                try:
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
    st.empty()

if supabase_helper:
    st.subheader("📸 Snapshots do Repositório")
    
    def get_parquet_snapshots(repo_name, quarter):
        try:
            snapshots = supabase_helper.list_parquet_snapshots(repo_name=repo_name, quarter=quarter)
            return snapshots
        except Exception as e:
            st.error(f"Erro ao carregar snapshots: {e}")
            return []
    
    try:
        with st.spinner("Carregando snapshots..."):
            parquet_snapshots = get_parquet_snapshots(selected_repo, selected_quarter)
            
        if parquet_snapshots:
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
            
            selected_snapshot_display = st.selectbox(
                "Selecionar Snapshot para Análise",
                ["Selecione um snapshot..."] + snapshot_options
            )
            
            if selected_snapshot_display != "Selecione um snapshot...":
                selected_snapshot = snapshot_dict[selected_snapshot_display]
                snapshot_id = selected_snapshot.get('snapshot_id', '')
                
                commits_key = f"snapshot_commits_{snapshot_id}"
                prs_key = f"snapshot_prs_{snapshot_id}"
                
                if commits_key not in st.session_state:
                    with st.spinner("Carregando dados automaticamente..."):
                        try:
                            commits_df = supabase_helper.load_snapshot_data(snapshot_id, 'commits', selected_quarter)
                            if commits_df is not None:
                                st.session_state[commits_key] = commits_df
                                st.session_state["current_snapshot_id"] = snapshot_id
                            
                            prs_df = supabase_helper.load_snapshot_data(snapshot_id, 'pull_requests', selected_quarter)
                            if prs_df is not None:
                                st.session_state[prs_key] = prs_df
                            
                            st.success("✅ Dados carregados automaticamente!")
                            
                        except Exception as e:
                            st.error(f"Erro ao carregar dados: {e}")
                
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

if supabase_helper:
    st.subheader("📊 Análise dos Dados")
    
    current_snapshot_id = st.session_state.get("current_snapshot_id")
    if current_snapshot_id:
        commits_key = f"snapshot_commits_{current_snapshot_id}"
        
        if commits_key in st.session_state:
            commits_df = st.session_state[commits_key].copy()
            
            if 'date' in commits_df.columns:
                commits_df['date'] = pd.to_datetime(commits_df['date'])
                commits_df['date_only'] = commits_df['date'].dt.date
            
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
            
            if 'message' in commits_df.columns:
                commits_df['commit_type'] = commits_df['message'].apply(extract_commit_type)
            
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
                
                filtered_commits = commits_df[
                    (commits_df['date_only'] >= start_date) & 
                    (commits_df['date_only'] <= end_date)
                ].copy()
            else:
                st.warning("Não foi possível detectar datas nos commits")
                filtered_commits = commits_df.copy()
                start_date = end_date = None
            
            if len(filtered_commits) > 0:
                st.subheader("📈 KPIs por Tipo de Commit")
                if 'commit_type' in filtered_commits.columns:
                    commit_counts = filtered_commits['commit_type'].value_counts()
                    
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
                    
                    cols = st.columns(4)
                    for i, (commit_type, count) in enumerate(commit_counts.head(8).items()):
                        with cols[i % 4]:
                            display_name = type_names.get(commit_type, commit_type.title())
                            st.metric(display_name, count)
                
                st.subheader("🥧 Distribuição de Tipos de Commits")
                if 'commit_type' in filtered_commits.columns and len(filtered_commits) > 0:
                    commit_counts = filtered_commits['commit_type'].value_counts()
                    
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
                                
                                if 'commit_type' in author_commits.columns:
                                    author_types = author_commits['commit_type'].value_counts()
                                    st.write("**Tipos de commits:**")
                                    for commit_type, count in author_types.items():
                                        display_name = type_names.get(commit_type, commit_type.title())
                                        st.write(f"• {display_name}: {count}")
                            
                            with col2:
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