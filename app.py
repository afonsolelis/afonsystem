import os
import sys
import subprocess
import shutil


def _bootstrap_env_and_reexec_if_needed():
    # Avoid infinite loop
    if os.environ.get("AFONSYSTEM_BOOTSTRAPPED") == "1":
        # Also ensure we don't leak user site packages
        os.environ.setdefault("PYTHONNOUSERSITE", "1")
        return

    project_root = os.path.dirname(os.path.abspath(__file__))
    venv_dir = os.path.join(project_root, ".venv")
    venv_python = os.path.join(venv_dir, "bin", "python")

    # Detect if we're already using the project's venv
    in_project_venv = os.path.realpath(sys.prefix).startswith(os.path.realpath(venv_dir))
    using_user_site = any(p.startswith(os.path.expanduser("~/.local/lib")) for p in sys.path)

    if in_project_venv and not using_user_site:
        os.environ.setdefault("PYTHONNOUSERSITE", "1")
        return

    # Prefer Python 3.12, then 3.13, then current interpreter
    candidates = ["python3.12", "python3.13", sys.executable]
    chosen = None
    for cmd in candidates:
        if shutil.which(cmd):
            chosen = cmd
            break

    if chosen is None:
        # If we can't find a python, continue as-is (will likely error), but at least avoid user site
        os.environ.setdefault("PYTHONNOUSERSITE", "1")
        return

    # Create venv if missing
    if not os.path.isdir(venv_dir):
        try:
            subprocess.run([chosen, "-m", "venv", venv_dir], check=True)
        except Exception:
            # As a fallback, try virtualenv if available
            if shutil.which("virtualenv"):
                subprocess.run(["virtualenv", "-p", chosen, venv_dir], check=True)
            else:
                # Can't create venv, continue but isolate user site
                os.environ.setdefault("PYTHONNOUSERSITE", "1")
                return

    # Install requirements to the venv (best effort)
    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    reqs = os.path.join(project_root, "requirements.txt")
    if os.path.isfile(reqs):
        try:
            subprocess.run([venv_python, "-m", "pip", "install", "--upgrade", "pip"], check=True, env=env)
            subprocess.run([venv_python, "-m", "pip", "install", "-r", reqs], check=True, env=env)
        except Exception:
            pass

    # Re-exec using the venv's python to run Streamlit
    env["AFONSYSTEM_BOOTSTRAPPED"] = "1"
    env["PYTHONNOUSERSITE"] = "1"
    app_path = os.path.join(project_root, "app.py")
    os.execv(venv_python, [venv_python, "-m", "streamlit", "run", app_path])


_bootstrap_env_and_reexec_if_needed()

import streamlit as st
import logging
from helpers import *
from helpers.ui_components import render_all_pull_requests_table

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

setup_page_config()

collector = get_data_collector()
snapshot_manager = get_snapshot_manager()

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
    render_snapshot_creation_button(selected_repo, collector, snapshot_manager, selected_quarter)
with col2:
    st.empty()

if snapshot_manager:
    selected_snapshot = render_snapshot_selector(snapshot_manager, selected_repo, selected_quarter)

    if selected_snapshot:
        snapshot_id = selected_snapshot.get('snapshot_id', '')

        commits_key = f"snapshot_commits_{snapshot_id}"
        prs_key = f"snapshot_prs_{snapshot_id}"

        if commits_key not in st.session_state:
            with st.spinner("Carregando dados automaticamente..."):
                try:
                    commits_df = snapshot_manager.load_snapshot_data(snapshot_id, 'commits', selected_quarter)
                    if commits_df is not None:
                        st.session_state[commits_key] = commits_df
                        st.session_state["current_snapshot_id"] = snapshot_id

                    prs_df = snapshot_manager.load_snapshot_data(snapshot_id, 'pull_requests', selected_quarter)
                    if prs_df is not None:
                        st.session_state[prs_key] = prs_df

                    st.success("✅ Dados carregados automaticamente!")

                except Exception as e:
                    st.error(f"Erro ao carregar dados: {e}")

        render_snapshot_metrics(selected_snapshot)

st.divider()

if snapshot_manager:
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

                    # Render pull requests table with fallback mechanism
                    # Note: This function sometimes fails in Streamlit Cloud deployment due to import issues
                    # We've implemented a fallback mechanism to handle this environment-specific problem
                    try:
                        render_all_pull_requests_table(prs_df)
                    except NameError:
                        # Fallback: try explicit import
                        try:
                            from helpers.ui_components import render_all_pull_requests_table
                            render_all_pull_requests_table(prs_df)
                        except Exception as fallback_error:
                            st.error("❌ Erro ao renderizar a tabela de pull requests.")
                            st.info("💡 Esta é uma falha conhecida que ocorre apenas no ambiente de deploy.")
                            if st.checkbox("Mostrar detalhes do erro"):
                                st.write("Erro de importação:", str(fallback_error))
                    except Exception as e:
                        st.error("❌ Erro inesperado ao renderizar a tabela de pull requests.")
                        st.info("💡 O restante da análise está funcionando normalmente.")
                        if st.checkbox("Mostrar detalhes do erro"):
                            st.write("Erro detalhado:", str(e))
                else:
                    st.info("Nenhum pull request encontrado para este snapshot.")
            else:
                st.info("Dados de pull requests não carregados para este snapshot.")

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
    st.error("❌ Gerenciador de snapshots indisponível.")
