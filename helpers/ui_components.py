import streamlit as st
import pandas as pd
import plotly.express as px
from .data_analysis import get_commit_type_display_names
from .snapshot_manager import SnapshotManager

def render_snapshot_creation_button(selected_repo, collector, snapshot_manager: SnapshotManager, selected_quarter):
    """Render the snapshot creation button and handle its logic"""

    if st.button("🚀 Criar Snapshot", type="primary"):
        if snapshot_manager:
            with st.spinner(f"Criando snapshot para {selected_repo}..."):
                progress_container = st.empty()

                def progress_callback(message: str):
                    progress_container.info(message)

                try:
                    snapshot_id = collector.collect_and_create_snapshot(selected_repo, progress_callback, selected_quarter)

                    if snapshot_id:
                        st.success(f"🚀 Snapshot criado com sucesso!")
                        st.info(f"🆔 ID do Snapshot: {snapshot_id}")
                        # Invalida caches de snapshots para forçar recarregamento
                        try:
                            keys_to_delete = [k for k in st.session_state.keys() if k.startswith("snapshots_cache::")]
                            for k in keys_to_delete:
                                del st.session_state[k]
                        except Exception:
                            pass
                        st.rerun()
                    else:
                        st.error("❌ Falha ao criar snapshot")

                except Exception as e:
                    st.error(f"❌ Erro ao criar snapshot: {e}")

def render_snapshot_selector(snapshot_manager: SnapshotManager, selected_repo, selected_quarter):
    """Render snapshot selector and return selected snapshot"""
    st.subheader("📸 Snapshots do Repositório")

    try:
        with st.spinner("Carregando snapshots..."):
            parquet_snapshots = snapshot_manager.list_repository_snapshots(repo_name=selected_repo, quarter=selected_quarter)

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
                return snapshot_dict[selected_snapshot_display]
        else:
            st.info(f"Nenhum snapshot encontrado para {selected_repo}")
            st.write("💡 Crie seu primeiro snapshot usando o botão acima!")

    except Exception as e:
        st.error(f"Erro ao carregar snapshots: {e}")

    return None

def render_snapshot_metrics(snapshot):
    """Render snapshot metrics"""
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📝 Commits", snapshot.get('commits_count', 0))
    with col2:
        st.metric("🔀 Pull Requests", snapshot.get('pull_requests_count', 0))
    with col3:
        st.metric("📅 Data", snapshot.get('timestamp', 'N/A'))

def render_commit_type_kpis(filtered_commits):
    """Render KPIs for commit types"""
    st.subheader("📈 KPIs por Tipo de Commit")
    if 'commit_type' in filtered_commits.columns:
        commit_counts = filtered_commits['commit_type'].value_counts()
        type_names = get_commit_type_display_names()

        cols = st.columns(4)
        for i, (commit_type, count) in enumerate(commit_counts.head(8).items()):
            with cols[i % 4]:
                display_name = type_names.get(commit_type, commit_type.title())
                st.metric(display_name, count)

def render_commit_type_pie_chart(filtered_commits):
    """Render pie chart for commit types"""
    st.subheader("🥧 Distribuição de Tipos de Commits")
    if 'commit_type' in filtered_commits.columns and len(filtered_commits) > 0:
        commit_counts = filtered_commits['commit_type'].value_counts()
        type_names = get_commit_type_display_names()

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

def render_daily_commits_chart(filtered_commits):
    """Render daily commits line chart"""
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

def render_date_filter(commits_df):
    """Render date filter controls and return selected dates"""
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

        return start_date, end_date
    else:
        st.warning("Não foi possível detectar datas nos commits")
        return None, None

def render_individual_analysis(filtered_commits):
    """Render individual author analysis"""
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
                type_names = get_commit_type_display_names()

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

def render_all_commits_table(filtered_commits):
    """Render all commits table"""
    st.subheader("📝 Todos os Commits")
    if len(filtered_commits) > 0:
        # Sort by date descending (most recent first)
        sorted_commits = filtered_commits.sort_values('date', ascending=False)
        display_commits = sorted_commits[['date', 'author', 'message']].copy()

        # Se há URL, adiciona coluna link clicável
        if 'url' in filtered_commits.columns:
            display_commits['link'] = sorted_commits['url'].values

        st.dataframe(display_commits, use_container_width=True, column_config={
            'link': st.column_config.LinkColumn('Link', display_text="Abrir")
        } if 'url' in filtered_commits.columns else None)

        # Show total count
        total_commits = len(filtered_commits)
        st.caption(f"Mostrando todos os {total_commits} commits.")
    else:
        st.dataframe(pd.DataFrame(), use_container_width=True)

def render_pull_request_metrics(prs_df):
    """Render pull request metrics"""
    if prs_df is None or len(prs_df) == 0:
        st.info("Nenhum pull request encontrado")
        return

    st.subheader("🔀 Análise de Pull Requests")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_prs = len(prs_df)
        st.metric("Total de PRs", total_prs)

    with col2:
        merged_prs = len(prs_df[prs_df['state'] == 'closed']) if 'state' in prs_df.columns else 0
        st.metric("PRs Fechados", merged_prs)

    with col3:
        open_prs = len(prs_df[prs_df['state'] == 'open']) if 'state' in prs_df.columns else 0
        st.metric("PRs Abertos", open_prs)

    with col4:
        unique_authors = prs_df['author'].nunique() if 'author' in prs_df.columns else 0
        st.metric("Autores", unique_authors)

def render_pull_request_state_chart(prs_df):
    """Render pull request state distribution chart"""
    if prs_df is None or len(prs_df) == 0 or 'state' not in prs_df.columns:
        return

    st.subheader("📊 Distribuição de Estados dos PRs")

    state_counts = prs_df['state'].value_counts()
    state_data = pd.DataFrame({
        'Estado': ['Aberto' if state == 'open' else 'Fechado' for state in state_counts.index],
        'Quantidade': state_counts.values
    })

    fig_pie = px.pie(
        state_data,
        values='Quantidade',
        names='Estado',
        title="Distribuição dos Estados dos Pull Requests",
        color_discrete_map={'Aberto': '#ff6b6b', 'Fechado': '#51cf66'}
    )
    st.plotly_chart(fig_pie, use_container_width=True)

def render_pull_request_authors_chart(prs_df):
    """Render pull request authors chart"""
    if prs_df is None or len(prs_df) == 0 or 'author' not in prs_df.columns:
        return

    st.subheader("👥 PRs por Autor")

    author_counts = prs_df['author'].value_counts().head(10)
    author_data = pd.DataFrame({
        'Autor': author_counts.index,
        'Quantidade': author_counts.values
    })

    fig_bar = px.bar(
        author_data,
        x='Autor',
        y='Quantidade',
        title="Top 10 Autores por Número de Pull Requests"
    )
    fig_bar.update_layout(
        xaxis_title="Autor",
        yaxis_title="Número de PRs"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

def render_pull_request_timeline(prs_df):
    """Render pull request timeline chart"""
    if prs_df is None or len(prs_df) == 0 or 'created_at' not in prs_df.columns:
        return

    st.subheader("📈 Timeline de Pull Requests")

    prs_timeline = prs_df.copy()
    prs_timeline['created_at'] = pd.to_datetime(prs_timeline['created_at'])
    prs_timeline['date_only'] = prs_timeline['created_at'].dt.date

    daily_prs = prs_timeline.groupby('date_only').size().reset_index()
    daily_prs.columns = ['Data', 'PRs']

    fig_line = px.line(
        daily_prs,
        x='Data',
        y='PRs',
        title="Número de Pull Requests Criados por Dia",
        markers=True
    )
    fig_line.update_layout(
        xaxis_title="Data",
        yaxis_title="Número de PRs"
    )
    st.plotly_chart(fig_line, use_container_width=True)

def render_all_pull_requests_table(prs_df):
    """Render all pull requests table"""
    if prs_df is None or len(prs_df) == 0:
        return

    st.subheader("🔀 Todos os Pull Requests")

    # Sort by created_at descending (most recent first)
    sorted_prs = prs_df.sort_values('created_at', ascending=False)
    display_prs = sorted_prs[['created_at', 'author', 'title', 'state']].copy()

    # Se há URL, adiciona coluna link clicável
    if 'url' in prs_df.columns:
        display_prs['link'] = sorted_prs['url'].values

    st.dataframe(display_prs, use_container_width=True, column_config={
        'link': st.column_config.LinkColumn('Link', display_text="Abrir")
    } if 'url' in prs_df.columns else None)

    # Show total count
    total_prs = len(prs_df)
    st.caption(f"Mostrando todos os {total_prs} pull requests.")
