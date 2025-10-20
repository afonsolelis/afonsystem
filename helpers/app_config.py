import streamlit as st
from .data_collector import GitHubDataCollector
from .snapshot_manager import SnapshotManager
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource
def get_data_collector():
    collector = GitHubDataCollector()
    return collector

@st.cache_resource
def get_snapshot_manager():
    return SnapshotManager()

@st.cache_data
def get_available_repos():
    collector = get_data_collector()
    return collector.get_available_repos()

def setup_page_config():
    st.set_page_config(layout="wide")
    st.title("📊 Análise de Repositórios GitHub")

def get_available_quarters():
    return ["2025-2B"]
