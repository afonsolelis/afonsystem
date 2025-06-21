import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from helpers.data_collector import GitHubDataCollector
from helpers.database_helper import DatabaseHelper
from helpers.data_formatter import DataFormatter
from helpers.analytics_service import AnalyticsService
from dotenv import load_dotenv

load_dotenv()

# Layout
st.set_page_config(layout="wide")

# Initialize data collector
@st.cache_resource
def get_data_collector():
    return GitHubDataCollector()

collector = get_data_collector()

st.title("📊 GitHub Repository Analytics")

# Quarter filter
@st.cache_data
def get_available_quarters():
    """Extract quarters from datalake folder names"""
    quarters = set()
    datalake_path = "datalake"
    if os.path.exists(datalake_path):
        for folder in os.listdir(datalake_path):
            folder_path = os.path.join(datalake_path, folder)
            if os.path.isdir(folder_path):
                # Extract quarter from folder name (e.g., inteli_college_2025_1b_...)
                parts = folder.split('_')
                if len(parts) >= 4 and parts[0] == 'inteli' and parts[1] == 'college':
                    year = parts[2]
                    semester = parts[3]
                    quarter = f"{year}-{semester.upper()}"
                    quarters.add(quarter)
    return sorted(list(quarters))

available_quarters = get_available_quarters()
if not available_quarters:
    st.error("No quarters found in datalake folder")
    st.stop()

selected_quarter = st.selectbox("Select Quarter", available_quarters)

# Get repositories for selected quarter
@st.cache_data
def get_repos_for_quarter(quarter):
    """Get repositories for a specific quarter"""
    repos = []
    year, semester = quarter.split('-')
    semester_lower = semester.lower()
    
    datalake_path = "datalake"
    if os.path.exists(datalake_path):
        for folder in os.listdir(datalake_path):
            folder_path = os.path.join(datalake_path, folder)
            if os.path.isdir(folder_path):
                # Check if folder matches the selected quarter
                if f"_{year}_{semester_lower}_" in folder:
                    repos.append(folder)
    return sorted(repos)

available_repos = get_repos_for_quarter(selected_quarter)
if not available_repos:
    st.error(f"No repositories found for quarter {selected_quarter}")
    st.stop()

selected_repo = st.selectbox("Select Repository", available_repos)

# Update button
if st.button("🔄 Update Data Now", type="primary"):
    with st.spinner(f"Collecting data for {selected_repo}..."):
        try:
            db_path = collector.create_timestamped_db(selected_repo)
            if db_path:
                st.success(f"✅ Data updated! Created: {db_path}")
                st.rerun()
            else:
                st.error("❌ Failed to update data")
        except Exception as e:
            st.error(f"❌ Error updating data: {e}")

st.divider()

# Filters section
st.subheader("🔧 Filters")

# Database selection
@st.cache_data
def get_databases_for_repo(repo_name):
    """Get available databases for a repository"""
    repo_path = f"datalake/{repo_name}"
    databases = []
    
    if os.path.exists(repo_path):
        for file in os.listdir(repo_path):
            if file.endswith('.duckdb'):
                databases.append(file)
    
    return sorted(databases, reverse=True)  # Most recent first

available_dbs = get_databases_for_repo(selected_repo)
if available_dbs:
    # Show available snapshots for selected repo
    db_options = DataFormatter.format_database_options(available_dbs)
    
    selected_db_index = st.selectbox("Select Database Snapshot", range(len(db_options)), format_func=lambda x: db_options[x])
    selected_db = available_dbs[selected_db_index]
else:
    st.warning(f"No databases found for {selected_repo}. Click 'Update Data Now' to create one.")
    st.stop()

# Connect to selected database
db_path = f"datalake/{selected_repo}/{selected_db}"
if not os.path.exists(db_path):
    st.error(f"Database file not found: {db_path}")
    st.stop()

# Check database tables
@st.cache_data
def get_table_info(db_path):
    with DatabaseHelper(db_path) as db_helper:
        return db_helper.get_table_info()

table_info = get_table_info(db_path)
has_commits = table_info['has_commits']
has_prs = table_info['has_prs']

if not has_commits and not has_prs:
    st.error("No data tables found in the database")
    st.stop()

# Cache analytics functions
@st.cache_data
def get_date_range(db_path):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_date_range()

@st.cache_data
def get_commit_kpis(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_commit_kpis(start_date, end_date)

@st.cache_data
def get_commits_data(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_commits_data(start_date, end_date)

@st.cache_data
def get_pull_requests_data(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_pull_requests_data(start_date, end_date)

@st.cache_data
def get_commits_by_author_chart_data(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_commits_by_author_chart_data(start_date, end_date)

@st.cache_data
def get_commits_by_type_chart_data(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_commits_by_type_chart_data(start_date, end_date)

@st.cache_data
def get_daily_commits_chart_data(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        return analytics.get_daily_commits_chart_data(start_date, end_date)

@st.cache_data
def get_daily_commits_by_author(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        # Custom query to get daily commits by author
        result = analytics.commit_repo.conn.execute("""
            SELECT DATE(date) as day, author, COUNT(*) as count
            FROM commits
            WHERE date BETWEEN ? AND ?
            GROUP BY DATE(date), author
            ORDER BY day, author
        """, [start_date, end_date])
        return result.fetchall()

@st.cache_data
def get_authors_list(db_path, start_date, end_date):
    with AnalyticsService(db_path) as analytics:
        result = analytics.commit_repo.conn.execute("""
            SELECT DISTINCT author
            FROM commits
            WHERE date BETWEEN ? AND ?
            ORDER BY author
        """, [start_date, end_date])
        return [row[0] for row in result.fetchall()]

# Date range filter
if has_commits:
    date_range = get_date_range(db_path)
    if date_range and date_range[0] and date_range[1]:
        min_date = pd.to_datetime(date_range[0]).date()
        max_date = pd.to_datetime(date_range[1]).date()
        
        selected_dates = st.date_input(
            "Select Date Range", 
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
        else:
            start_date = end_date = selected_dates
    else:
        start_date = end_date = datetime.now().date()

st.divider()

# KPIs Section
if has_commits:
    st.header("📈 KPIs")
    
    kpis = get_commit_kpis(db_path, start_date, end_date)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Commits", kpis['total_commits'])
    col2.metric("Features", kpis['feat_commits'])
    col3.metric("Fixes", kpis['fix_commits'])
    col4.metric("Docs", kpis['docs_commits'])
    
    col5, col6, col7 = st.columns(3)
    col5.metric("Chores", kpis['chore_commits'])
    col6.metric("Refactors", kpis['refactor_commits'])
    col7.metric("Tests", kpis['test_commits'])

# Commits Table
if has_commits:
    st.header("📝 Commits")
    commits_data = get_commits_data(db_path, start_date, end_date)
    
    if commits_data:
        commits_df = DataFormatter.format_commits_for_display(commits_data)
        st.dataframe(commits_df, use_container_width=True)
    else:
        st.info("No commits found in selected date range")

# Pull Requests Table
if has_prs:
    st.header("🔀 Pull Requests")
    prs_data = get_pull_requests_data(db_path, start_date, end_date)
    
    if prs_data:
        prs_df = DataFormatter.format_pull_requests_for_display(prs_data)
        st.dataframe(prs_df, use_container_width=True)
    else:
        st.info("No pull requests found in selected date range")

# Charts Section
if has_commits:
    st.header("📊 Charts")
    
    # Commits by Author
    authors_data = get_commits_by_author_chart_data(db_path, start_date, end_date)
    if authors_data:
        authors_df = pd.DataFrame(authors_data, columns=['author', 'count'])
        fig_authors = px.bar(
            authors_df, 
            x='author', 
            y='count', 
            title="Commits by Author"
        )
        st.plotly_chart(fig_authors, use_container_width=True)
    
    # Commits by Type
    types_data = get_commits_by_type_chart_data(db_path, start_date, end_date)
    if types_data:
        types_df = pd.DataFrame(types_data, columns=['commit_type', 'count'])
        fig_types = px.pie(
            types_df, 
            names='commit_type', 
            values='count', 
            title="Commit Types Distribution"
        )
        st.plotly_chart(fig_types, use_container_width=True)
    
    # Daily Commits Timeline
    daily_data = get_daily_commits_chart_data(db_path, start_date, end_date)
    if daily_data:
        daily_df = pd.DataFrame(daily_data, columns=['day', 'count'])
        fig_daily = px.line(
            daily_df, 
            x='day', 
            y='count', 
            title="Daily Commits Timeline",
            markers=True
        )
        st.plotly_chart(fig_daily, use_container_width=True)
    
    # Daily Commits by Student
    st.subheader("📊 Daily Commits by Student")
    
    # Get list of authors for selection
    authors_list = get_authors_list(db_path, start_date, end_date)
    if authors_list:
        selected_authors = st.multiselect(
            "Select Students",
            authors_list,
            default=authors_list[:5] if len(authors_list) > 5 else authors_list  # Select first 5 by default
        )
        
        if selected_authors:
            daily_by_author_data = get_daily_commits_by_author(db_path, start_date, end_date)
            if daily_by_author_data:
                daily_by_author_df = pd.DataFrame(daily_by_author_data, columns=['day', 'author', 'count'])
                
                # Filter by selected authors
                filtered_df = daily_by_author_df[daily_by_author_df['author'].isin(selected_authors)]
                
                if not filtered_df.empty:
                    fig_daily_by_author = px.line(
                        filtered_df,
                        x='day',
                        y='count',
                        color='author',
                        title="Daily Commits Timeline by Student",
                        markers=True
                    )
                    fig_daily_by_author.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Number of Commits",
                        legend_title="Student"
                    )
                    st.plotly_chart(fig_daily_by_author, use_container_width=True)
                else:
                    st.info("No data available for selected students in the date range.")
            else:
                st.info("No daily commit data available.")
        else:
            st.info("Please select at least one student to view the timeline.")
    else:
        st.info("No authors found in the selected date range.")