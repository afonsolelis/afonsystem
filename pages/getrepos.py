import streamlit as st
import time
import json
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv
import clickhouse_connect
import duckdb
from datetime import datetime
import os
import logging

# Configuration
st.set_page_config(page_title="GitHub ETL to ClickHouse", layout="wide")
load_dotenv(dotenv_path=".env")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPOS_JSON = os.getenv("GITREPOS")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_clickhouse():
    """Establish connection to ClickHouse database."""
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="afonsystem"
    )

def get_github_client(token: str) -> Github:
    """Initialize GitHub API client with authentication token."""
    return Github(token)

def fetch_repositories_from_env() -> list[str]:
    """Parse repository list from environment variables."""
    try:
        return json.loads(REPOS_JSON)
    except Exception as e:
        st.error("Error parsing GITREPOS from environment. Please verify JSON format.")
        logger.error(f"Failed to parse repositories: {e}")
        raise e

def get_existing_commits(client, repo_name: str) -> set:
    """Retrieve existing commit SHAs from ClickHouse to avoid duplicates."""
    try:
        result = client.query(f"SELECT sha FROM commits WHERE repo_name = '{repo_name}'")
        return set(row[0] for row in result.result_rows)
    except Exception as e:
        logger.warning(f"Could not fetch existing commits for {repo_name}: {e}")
        return set()

def get_existing_prs(client, repo_name: str) -> set:
    """Retrieve existing pull request numbers from ClickHouse to avoid duplicates."""
    try:
        result = client.query(f"SELECT number FROM pull_requests WHERE repo_name = '{repo_name}'")
        return set(str(row[0]) for row in result.result_rows)
    except Exception as e:
        logger.warning(f"Could not fetch existing PRs for {repo_name}: {e}")
        return set()

def get_new_commits(repo_name: str, github_client: Github, click_client) -> pd.DataFrame:
    """Fetch new commits from GitHub API, excluding those already in database."""
    existing_shas = get_existing_commits(click_client, repo_name)
    st.write(f"Found {len(existing_shas)} existing commits in database")
    
    data = []
    repo = github_client.get_repo(repo_name)
    
    for commit in repo.get_commits():
        if commit.sha not in existing_shas:
            author = commit.commit.author or {}
            data.append({
                "sha": commit.sha,
                "message": commit.commit.message,
                "author": getattr(author, "name", "") or "",
                "email": getattr(author, "email", "") or "",
                "date": getattr(author, "date", None).isoformat() if getattr(author, "date", None) else None,
                "url": commit.html_url,
                "repo_name": repo_name
            })
    
    st.write(f"Identified {len(data)} new commits to process")
    return pd.DataFrame(data)

def get_new_pull_requests(repo_name: str, github_client: Github, click_client) -> pd.DataFrame:
    """Fetch new pull requests from GitHub API, excluding those already in database."""
    existing_prs = get_existing_prs(click_client, repo_name)
    st.write(f"Found {len(existing_prs)} existing pull requests in database")
    
    data = []
    repo = github_client.get_repo(repo_name)
    
    for pr in repo.get_pulls(state="all", sort="created", direction="desc"):
        if str(pr.number) not in existing_prs:
            data.append({
                "number": str(pr.number),
                "title": pr.title,
                "author": pr.user.login,
                "email": pr.user.email or "",
                "created_at": pr.created_at.isoformat() if pr.created_at else None,
                "state": pr.state,
                "comments": int(pr.comments),
                "review_comments": int(pr.review_comments),
                "commits": len(list(pr.get_commits())),
                "url": pr.html_url,
                "repo_name": repo_name
            })
    
    st.write(f"Identified {len(data)} new pull requests to process")
    return pd.DataFrame(data)

def insert_clickhouse(client, df: pd.DataFrame, table: str) -> int:
    """Insert DataFrame records into specified ClickHouse table."""
    if df.empty:
        st.info("No new data to insert")
        return 0

    try:
        if table == "commits":
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            client.insert(
                table,
                df[["sha", "message", "author", "email", "date", "url", "repo_name"]].values.tolist(),
                column_names=["sha", "message", "author", "email", "date", "url", "repo_name"]
            )

        elif table == "pull_requests":
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            client.insert(
                table,
                df[["number", "title", "author", "email", "created_at", "state",
                    "comments", "review_comments", "commits", "url", "repo_name"]].values.tolist(),
                column_names=["number", "title", "author", "email", "created_at", "state",
                              "comments", "review_comments", "commits", "url", "repo_name"]
            )

        logger.info(f"Successfully inserted {len(df)} records into {table}")
        return len(df)
        
    except Exception as e:
        st.error(f"Failed to insert data into {table}: {str(e)}")
        logger.error(f"Insert operation failed: {e}")
        return 0

def optimize_tables(client):
    """Optimize ClickHouse tables for better performance."""
    with st.spinner("Optimizing database tables..."):
        try:
            client.command("OPTIMIZE TABLE commits FINAL")
            client.command("OPTIMIZE TABLE pull_requests FINAL")
            st.success("Table optimization completed successfully")
            logger.info("Database tables optimized")
        except Exception as e:
            st.error(f"Table optimization failed: {str(e)}")
            logger.error(f"Optimization failed: {e}")

def create_duckdb_backup(click_client) -> str:
    """Create a complete backup of ClickHouse data in DuckDB format."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Ensure backup directory exists
    backup_dir = "duckdb_exports"
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_filename = os.path.join(backup_dir, f"backup_github_data_{timestamp}.duckdb")
    
    with st.spinner(f"Creating DuckDB backup: {backup_filename}..."):
        try:
            conn = duckdb.connect(backup_filename)
            
            # Export commits table
            st.write("Exporting commits table...")
            commits_result = click_client.query("SELECT * FROM commits")
            commits_df = pd.DataFrame(commits_result.result_rows, columns=commits_result.column_names)
            conn.execute("CREATE TABLE commits AS SELECT * FROM commits_df")
            
            # Export pull_requests table
            st.write("Exporting pull_requests table...")
            prs_result = click_client.query("SELECT * FROM pull_requests")
            prs_df = pd.DataFrame(prs_result.result_rows, columns=prs_result.column_names)
            conn.execute("CREATE TABLE pull_requests AS SELECT * FROM prs_df")
            
            # Create backup metadata
            backup_info = pd.DataFrame([{
                'backup_timestamp': timestamp,
                'backup_date': datetime.now().isoformat(),
                'total_commits': len(commits_df),
                'total_prs': len(prs_df),
                'source_system': 'ClickHouse',
                'etl_version': '1.0'
            }])
            conn.execute("CREATE TABLE backup_metadata AS SELECT * FROM backup_info")
            
            conn.close()
            
            # Calculate backup statistics
            file_size_mb = os.path.getsize(backup_filename) / (1024 * 1024)
            
            st.success(f"""
            **Backup created successfully**
            
            **File:** {backup_filename}  
            **Commits exported:** {len(commits_df):,}  
            **Pull requests exported:** {len(prs_df):,}  
            **File size:** {file_size_mb:.2f} MB  
            **Timestamp:** {timestamp}
            """)
            
            logger.info(f"Backup created: {backup_filename} ({file_size_mb:.2f} MB)")
            return backup_filename
            
        except Exception as e:
            error_msg = f"Backup creation failed: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
            return None

def verify_backup_integrity(backup_filename: str) -> bool:
    """Verify the integrity and completeness of the DuckDB backup."""
    try:
        conn = duckdb.connect(backup_filename, read_only=True)
        
        # Get table information
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        backup_stats = {}
        for table_name in table_names:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            backup_stats[table_name] = count
        
        conn.close()
        
        # Display verification results
        st.info(f"""
        **Backup integrity verification**
        
        **Tables found:** {', '.join(table_names)}  
        **Record counts:**  
        {chr(10).join([f'  - {table}: {count:,}' for table, count in backup_stats.items()])}
        """)
        
        logger.info(f"Backup verification completed for {backup_filename}")
        return True
        
    except Exception as e:
        error_msg = f"Backup verification failed: {str(e)}"
        st.error(error_msg)
        logger.error(error_msg)
        return False

# Streamlit Interface
st.title("GitHub ETL Pipeline with ClickHouse Storage")
st.markdown("Extract GitHub repository data and store in ClickHouse with automated DuckDB backup")

with st.sidebar:
    st.subheader("Repository Configuration")
    repos = fetch_repositories_from_env()

    search_term = st.text_input("Filter repositories:")
    if search_term:
        filtered_repos = [r for r in repos if search_term.lower() in r.lower()]
    else:
        filtered_repos = repos

    selected_repos = st.multiselect(
        "Select repositories to process:",
        filtered_repos,
        default=filtered_repos
    )

    st.divider()
    st.subheader("Backup Configuration")
    create_backup = st.checkbox("Create DuckDB backup after ETL", value=True)
    verify_backup_integrity_flag = st.checkbox("Verify backup integrity", value=True)

    start_button = st.button("Start ETL Process", type="primary")

if start_button and selected_repos:
    st.info(f"Starting ETL process for {len(selected_repos)} repositories...")
    
    github_client = get_github_client(GITHUB_TOKEN)
    click_client = connect_clickhouse()
    start_time = time.time()

    total_new_commits = 0
    total_new_prs = 0

    # Process each repository
    for repo in selected_repos:
        with st.expander(f"Processing repository: {repo}"):
            try:
                st.write("Processing commits...")
                commits = get_new_commits(repo, github_client, click_client)
                n_commits = insert_clickhouse(click_client, commits, "commits")
                total_new_commits += n_commits
                if n_commits > 0:
                    st.success(f"Inserted {n_commits} new commits")

            except GithubException as e:
                st.error(f"Error processing commits: {e}")
                logger.error(f"Commit processing failed for {repo}: {e}")

            try:
                st.write("Processing pull requests...")
                prs = get_new_pull_requests(repo, github_client, click_client)
                n_prs = insert_clickhouse(click_client, prs, "pull_requests")
                total_new_prs += n_prs
                if n_prs > 0:
                    st.success(f"Inserted {n_prs} new pull requests")

            except GithubException as e:
                st.error(f"Error processing pull requests: {e}")
                logger.error(f"PR processing failed for {repo}: {e}")

    # Optimize tables if new data was inserted
    if total_new_commits > 0 or total_new_prs > 0:
        optimize_tables(click_client)

    # Create backup
    backup_filename = None
    if create_backup:
        st.divider()
        st.subheader("Creating Data Backup")
        backup_filename = create_duckdb_backup(click_client)
        
        # Verify backup integrity
        if backup_filename and verify_backup_integrity_flag:
            verify_backup_integrity(backup_filename)

    # Display final summary
    etl_duration = time.time() - start_time
    st.divider()
    st.success(f"""
    **ETL Process Completed** (Duration: {etl_duration:.2f} seconds)
    
    **Summary:**  
    - New commits processed: {total_new_commits}  
    - New pull requests processed: {total_new_prs}  
    {f"- Backup file: {backup_filename}" if backup_filename else ""}
    """)
    
    logger.info(f"ETL completed: {total_new_commits} commits, {total_new_prs} PRs in {etl_duration:.2f}s")

elif start_button:
    st.warning("Please select at least one repository to process.")

# Display system information
with st.expander("System Information"):
    st.write(f"**GitHub Token:** {'Configured' if GITHUB_TOKEN else 'Not configured'}")
    st.write(f"**Total repositories available:** {len(fetch_repositories_from_env()) if REPOS_JSON else 0}")
    st.write(f"**Current timestamp:** {datetime.now().isoformat()}")