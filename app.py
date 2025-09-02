import streamlit as st
from helpers import *

setup_page_config()

collector = get_data_collector()
supabase_helper = get_supabase_helper()

available_quarters = get_available_quarters()
selected_quarter = st.selectbox("Selecionar Trimestre", available_quarters)

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
    render_snapshot_creation_button(selected_repo, collector, supabase_helper, selected_quarter)
with col2:
    st.empty()

if supabase_helper:
    selected_snapshot = render_snapshot_selector(supabase_helper, selected_repo, selected_quarter)
    
    if selected_snapshot:
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
        
        render_snapshot_metrics(selected_snapshot)

st.divider()

if supabase_helper:
    st.subheader("📊 Análise dos Dados")
    
    current_snapshot_id = st.session_state.get("current_snapshot_id")
    if current_snapshot_id:
        commits_key = f"snapshot_commits_{current_snapshot_id}"
        
        if commits_key in st.session_state:
            commits_df = st.session_state[commits_key].copy()
            commits_df = process_commits_data(commits_df)
            
            start_date, end_date = render_date_filter(commits_df)
            
            if start_date and end_date:
                filtered_commits = filter_commits_by_date(commits_df, start_date, end_date)
            else:
                filtered_commits = commits_df.copy()
            
            if len(filtered_commits) > 0:
                render_commit_type_kpis(filtered_commits)
                render_commit_type_pie_chart(filtered_commits)
                render_daily_commits_chart(filtered_commits)
                render_individual_analysis(filtered_commits)
            else:
                st.warning(f"Nenhum commit encontrado no período selecionado ({start_date} a {end_date})")
            
            render_all_commits_table(filtered_commits)
            
            st.divider()
            
            prs_key = f"snapshot_prs_{current_snapshot_id}"
            if prs_key in st.session_state:
                prs_df = st.session_state[prs_key]
                if prs_df is not None and len(prs_df) > 0:
                    render_pull_request_metrics(prs_df)
                    render_pull_request_state_chart(prs_df)
                    render_pull_request_authors_chart(prs_df)
                    render_pull_request_timeline(prs_df)
                    render_all_pull_requests_table(prs_df)
            
    else:
        st.info("💡 Selecione um snapshot acima para visualizar análises e gráficos.")
        st.write("Após selecionar um snapshot, você poderá ver:")
        st.write("• 📈 KPIs por tipo de commit")
        st.write("• 🥧 Gráfico de pizza dos tipos de commits")
        st.write("• 📈 Gráfico de linha de commits por dia")
        st.write("• 🗓️ Filtro por período de sprint")
        st.write("• 👥 Análise individual por aluno")
        st.write("• 🔀 Análises de Pull Requests")
        st.write("• 📊 Métricas e gráficos de PRs")
else:
    st.error("❌ Supabase não configurado. Verifique seu arquivo .env.")