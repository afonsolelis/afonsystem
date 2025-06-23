import streamlit as st
import pandas as pd
import time
from datetime import datetime
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

st.title("📊 GitHub Repository Analytics")

# Quarter filter - simplified for Supabase-only mode
available_quarters = ["2025-1B"]
selected_quarter = st.selectbox("Select Quarter", available_quarters)

# Get repositories from environment configuration with caching
@st.cache_data
def get_available_repos():
    return collector.get_available_repos()

env_repos = get_available_repos()
if env_repos:
    available_repos = env_repos
else:
    st.error("No repositories configured. Please set REPO_NAMES in your .env file.")
    st.code("REPO_NAMES=owner/repo1,owner/repo2")
    st.stop()

selected_repo = st.selectbox("Select Repository", available_repos)

# Snapshot button - simplified for Supabase-only mode
col1, col2 = st.columns(2)

with col1:
    if st.button("🚀 Create Snapshot", type="primary"):
        if supabase_helper:
            with st.spinner(f"Creating snapshot for {selected_repo}..."):
                # Create a progress container
                progress_container = st.empty()
                
                def progress_callback(message: str):
                    progress_container.info(message)
                
                try:
                    # Use the collect_and_create_snapshot method with selected quarter
                    snapshot_id = collector.collect_and_create_snapshot(selected_repo, progress_callback, selected_quarter)
                    
                    if snapshot_id:
                        st.success(f"🚀 Snapshot created successfully!")
                        st.info(f"🆔 Snapshot ID: {snapshot_id}")
                        st.rerun()
                    else:
                        st.error("❌ Failed to create snapshot")
                        
                except Exception as e:
                    st.error(f"❌ Error creating snapshot: {e}")
        else:
            st.error("❌ Supabase not configured properly")

with col2:
    st.info("📸 Snapshots are stored as Parquet files in Supabase for better performance and cloud accessibility.")

# Show existing snapshots for the selected repository
if supabase_helper:
    st.subheader("📸 Repository Snapshots")
    
    # Only show Parquet snapshots - use cache to avoid slow loads
    @st.cache_data(ttl=60)  # Cache for 1 minute
    def get_parquet_snapshots(repo_name, quarter):
        try:
            print(f"[DEBUG] {time.time():.2f} - Loading snapshots for {repo_name} in quarter {quarter}")
            snapshots = supabase_helper.list_parquet_snapshots(repo_name, quarter)
            print(f"[DEBUG] {time.time():.2f} - Found {len(snapshots)} snapshots for {repo_name}")
            return snapshots
        except Exception as e:
            print(f"[DEBUG] {time.time():.2f} - Error loading snapshots: {e}")
            st.error(f"Error loading snapshots: {e}")
            return []
    
    try:
        with st.spinner("Loading snapshots..."):
            parquet_snapshots = get_parquet_snapshots(selected_repo, selected_quarter)
        if parquet_snapshots:
            st.write(f"Found {len(parquet_snapshots)} Parquet snapshot(s) for {selected_repo}:")
            
            for snapshot in parquet_snapshots:
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                
                with col1:
                    snapshot_id = snapshot.get('snapshot_id', 'Unknown')
                    timestamp = snapshot.get('timestamp', 'Unknown')
                    commits_count = snapshot.get('commits_count', 0)
                    prs_count = snapshot.get('pull_requests_count', 0)
                    
                    st.write(f"🗂️ **{snapshot_id}**")
                    st.write(f"📅 {timestamp} | 📝 {commits_count} commits | 🔀 {prs_count} PRs")
                
                with col2:
                    if st.button("📊 Load Data", key=f"load_{snapshot_id}"):
                        try:
                            # Load commits data from snapshot
                            commits_df = supabase_helper.load_snapshot_data(snapshot_id, 'commits', selected_quarter)
                            if commits_df is not None:
                                st.session_state[f"snapshot_commits_{snapshot_id}"] = commits_df
                                st.success("✅ Commits data loaded!")
                            
                            # Load PRs data if available
                            prs_df = supabase_helper.load_snapshot_data(snapshot_id, 'pull_requests', selected_quarter)
                            if prs_df is not None:
                                st.session_state[f"snapshot_prs_{snapshot_id}"] = prs_df
                                st.success("✅ PRs data loaded!")
                            
                        except Exception as e:
                            st.error(f"Error loading data: {e}")
                
                with col3:
                    # Show loaded data
                    commits_key = f"snapshot_commits_{snapshot_id}"
                    prs_key = f"snapshot_prs_{snapshot_id}"
                    
                    if commits_key in st.session_state:
                        with st.expander(f"📝 Commits ({len(st.session_state[commits_key])})", expanded=False):
                            st.dataframe(st.session_state[commits_key].head(10), use_container_width=True)
                    
                    if prs_key in st.session_state:
                        with st.expander(f"🔀 PRs ({len(st.session_state[prs_key])})", expanded=False):
                            st.dataframe(st.session_state[prs_key].head(10), use_container_width=True)
                
                with col4:
                    if st.button(f"🗑️ Delete", key=f"delete_parquet_{snapshot_id}"):
                        try:
                            if supabase_helper.delete_parquet_snapshot(snapshot_id):
                                st.success("Parquet snapshot deleted!")
                                st.rerun()
                            else:
                                st.error("Failed to delete snapshot")
                        except Exception as e:
                            st.error(f"Error deleting snapshot: {e}")
                
                st.divider()
            
            # Show summary
            summary = supabase_helper.get_snapshot_summary(selected_repo)
            if summary:
                st.subheader("📊 Repository Summary")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Snapshots", summary.get('total_snapshots', 0))
                col2.metric("Total Commits", summary.get('total_commits', 0))
                col3.metric("Total PRs", summary.get('total_pull_requests', 0))
                col4.metric("Latest Snapshot", summary.get('latest_snapshot', {}).get('timestamp', 'None'))
                    
        else:
            st.info(f"No Parquet snapshots found for {selected_repo}")
            st.write("💡 Create your first Parquet snapshot using the button above!")
            
    except Exception as e:
        st.error(f"Error loading Parquet snapshots: {e}")

st.divider()

# Analytics from Parquet snapshots
if supabase_helper:
    st.subheader("📊 Analytics from Snapshots")
    st.info("💡 Load data from a snapshot above to view analytics and charts here.")
    st.write("Once you load data from a Parquet snapshot, you'll be able to see:")
    st.write("• 📈 KPIs and metrics")
    st.write("• 📝 Commit details")
    st.write("• 🔀 Pull request information")
    st.write("• 📊 Interactive charts and visualizations")
    st.write("• 👥 Student activity timelines")
else:
    st.error("❌ Supabase not configured. Please check your .env file.")