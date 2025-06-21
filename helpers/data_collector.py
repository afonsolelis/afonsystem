import os
import time
import re
from typing import List, Optional
from datetime import datetime
from github import Github
from dotenv import load_dotenv
from models.commit import Commit
from models.pull_request import PullRequest
from repositories.commit_repository import CommitRepository
from repositories.pull_request_repository import PullRequestRepository

load_dotenv()


class GitHubDataCollector:
    def __init__(self):
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.repo_names = self._get_repo_names()
        self.github = Github(self.github_token)
        self.datalake_path = 'datalake'
        
        # Create datalake directory if it doesn't exist
        os.makedirs(self.datalake_path, exist_ok=True)
    
    def _get_repo_names(self) -> List[str]:
        """Get repository names from environment variable"""
        repo_names_str = os.getenv('REPO_NAMES', '')
        if repo_names_str:
            return [name.strip() for name in repo_names_str.split(',')]
        return []
    
    def _repo_name_to_snake_case(self, repo_name: str) -> str:
        """Convert repository name to snake_case without special characters"""
        # Replace / with _
        snake_name = repo_name.replace('/', '_')
        # Replace - with _
        snake_name = snake_name.replace('-', '_')
        # Remove any other special characters except letters, numbers, and underscores
        snake_name = re.sub(r'[^a-zA-Z0-9_]', '', snake_name)
        # Convert to lowercase
        return snake_name.lower()
    
    def get_available_repos(self) -> List[str]:
        """Get list of available repositories from environment"""
        return self.repo_names
    
    def create_timestamped_db(self, repo_name: str) -> Optional[str]:
        """Create a timestamped database for a specific repository"""
        try:
            # Convert repo name to snake_case
            snake_repo_name = self._repo_name_to_snake_case(repo_name)
            
            # Create repository folder in datalake
            repo_folder = os.path.join(self.datalake_path, snake_repo_name)
            os.makedirs(repo_folder, exist_ok=True)
            
            # Create timestamped database file
            unix_timestamp = int(time.time())
            db_filename = f"{unix_timestamp}_{snake_repo_name}.duckdb"
            db_path = os.path.join(repo_folder, db_filename)
            
            # Collect data and save to database
            success = self._collect_repo_data(repo_name, db_path)
            
            if success:
                return db_path
            else:
                # Remove failed database file if it exists
                if os.path.exists(db_path):
                    os.remove(db_path)
                return None
                
        except Exception as e:
            print(f"Error creating timestamped database: {e}")
            return None
    
    def _collect_repo_data(self, repo_name: str, db_path: str) -> bool:
        """Collect commits and pull requests data for a repository"""
        try:
            # Get GitHub repository
            repo = self.github.get_repo(repo_name)
            
            # Initialize repositories
            commit_repo = CommitRepository(db_path)
            pr_repo = PullRequestRepository(db_path)
            
            # Create tables
            commit_repo.create_table()
            pr_repo.create_table()
            
            # Collect commits
            print(f"Collecting commits for {repo_name}...")
            commits = []
            for commit in repo.get_commits():
                try:
                    commit_data = Commit(
                        sha=commit.sha,
                        message=commit.commit.message,
                        author=commit.commit.author.name if commit.commit.author else "Unknown",
                        date=commit.commit.author.date if commit.commit.author else datetime.now(),
                        url=commit.html_url
                    )
                    commits.append(commit_data)
                except Exception as e:
                    print(f"Error processing commit {commit.sha}: {e}")
                    continue
            
            # Insert commits
            if commits:
                commit_repo.insert_commits(commits)
                print(f"Inserted {len(commits)} commits")
            
            # Collect pull requests
            print(f"Collecting pull requests for {repo_name}...")
            pull_requests = []
            for pr in repo.get_pulls(state='all'):
                try:
                    pr_data = PullRequest(
                        number=pr.number,
                        title=pr.title,
                        author=pr.user.login if pr.user else "Unknown",
                        state=pr.state,
                        created_at=pr.created_at,
                        url=pr.html_url
                    )
                    pull_requests.append(pr_data)
                except Exception as e:
                    print(f"Error processing PR #{pr.number}: {e}")
                    continue
            
            # Insert pull requests
            if pull_requests:
                pr_repo.insert_pull_requests(pull_requests)
                print(f"Inserted {len(pull_requests)} pull requests")
            
            # Close connections
            commit_repo.close()
            pr_repo.close()
            
            print(f"Successfully collected data for {repo_name}")
            return True
            
        except Exception as e:
            print(f"Error collecting data for {repo_name}: {e}")
            return False
    
    def get_available_databases(self) -> List[str]:
        """Get list of available database files from datalake"""
        databases = []
        
        if not os.path.exists(self.datalake_path):
            return databases
        
        # Walk through all repository folders in datalake
        for repo_folder in os.listdir(self.datalake_path):
            repo_path = os.path.join(self.datalake_path, repo_folder)
            
            if os.path.isdir(repo_path):
                # Look for .duckdb files in each repository folder
                for file in os.listdir(repo_path):
                    if file.endswith('.duckdb'):
                        # Return relative path from datalake
                        databases.append(os.path.join(repo_folder, file))
        
        # Sort by modification time (newest first)
        databases.sort(key=lambda x: os.path.getmtime(os.path.join(self.datalake_path, x)), reverse=True)
        
        return databases
    
    def get_databases_for_repo(self, repo_name: str) -> List[str]:
        """Get available databases for a specific repository"""
        snake_repo_name = self._repo_name_to_snake_case(repo_name)
        all_databases = self.get_available_databases()
        
        # Filter databases that belong to this repository
        repo_databases = [db for db in all_databases if db.startswith(snake_repo_name + '/')]
        
        return repo_databases