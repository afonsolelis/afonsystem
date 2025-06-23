import os
import time
import re
import pandas as pd
from typing import List, Optional, Dict, Callable
from datetime import datetime
from github import Github
from dotenv import load_dotenv
from .snapshot_manager import SnapshotManager

load_dotenv()


class GitHubDataCollector:
    def __init__(self):
        import time
        print(f"[DEBUG] {time.time():.2f} - GitHubDataCollector.__init__ start")
        
        self.github_token = os.getenv('GITHUB_TOKEN')
        print(f"[DEBUG] {time.time():.2f} - GitHub token loaded")
        
        self.repo_names = self._get_repo_names()
        print(f"[DEBUG] {time.time():.2f} - Repo names loaded: {len(self.repo_names)} repos")
        
        self._github = None  # Lazy initialization
        self.datalake_path = 'datalake'
        print(f"[DEBUG] {time.time():.2f} - Creating SnapshotManager")
        
        start_sm = time.time()
        self.snapshot_manager = SnapshotManager()
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager created in {time.time() - start_sm:.2f}s")
        
        # Create datalake directory if it doesn't exist
        os.makedirs(self.datalake_path, exist_ok=True)
        print(f"[DEBUG] {time.time():.2f} - GitHubDataCollector.__init__ complete")
    
    @property
    def github(self):
        """Lazy initialization of GitHub client"""
        if self._github is None:
            self._github = Github(self.github_token)
        return self._github
    
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
        """Create a timestamped parquet dataset for a specific repository"""
        try:
            # Convert repo name to snake_case
            snake_repo_name = self._repo_name_to_snake_case(repo_name)
            
            # Create repository folder in datalake
            repo_folder = os.path.join(self.datalake_path, snake_repo_name)
            os.makedirs(repo_folder, exist_ok=True)
            
            # Create timestamped dataset folder
            unix_timestamp = int(time.time())
            dataset_folder = f"{unix_timestamp}_{snake_repo_name}"
            dataset_path = os.path.join(repo_folder, dataset_folder)
            
            # Collect data and save to parquet files
            success = self._collect_repo_data(repo_name, dataset_path)
            
            if success:
                return dataset_path
            else:
                # Remove failed dataset folder if it exists
                if os.path.exists(dataset_path):
                    import shutil
                    shutil.rmtree(dataset_path)
                return None
                
        except Exception as e:
            print(f"Error creating timestamped dataset: {e}")
            return None
    
    def _collect_repo_data(self, repo_name: str, dataset_path: str) -> bool:
        """Collect commits and pull requests data for a repository and save as parquet"""
        try:
            # Get GitHub repository
            repo = self.github.get_repo(repo_name)
            
            # Create dataset directory
            os.makedirs(dataset_path, exist_ok=True)
            
            # Collect commits
            print(f"Collecting commits for {repo_name}...")
            commits_data = []
            for commit in repo.get_commits():
                try:
                    commits_data.append({
                        'sha': commit.sha,
                        'message': commit.commit.message,
                        'author': commit.commit.author.name if commit.commit.author else "Unknown",
                        'date': commit.commit.author.date if commit.commit.author else datetime.now(),
                        'url': commit.html_url
                    })
                except Exception as e:
                    print(f"Error processing commit {commit.sha}: {e}")
                    continue
            
            # Save commits to parquet
            if commits_data:
                commits_df = pd.DataFrame(commits_data)
                commits_file = os.path.join(dataset_path, 'commits.parquet')
                commits_df.to_parquet(commits_file, index=False)
                print(f"Saved {len(commits_data)} commits to parquet")
            
            # Collect pull requests
            print(f"Collecting pull requests for {repo_name}...")
            prs_data = []
            for pr in repo.get_pulls(state='all'):
                try:
                    prs_data.append({
                        'number': pr.number,
                        'title': pr.title,
                        'author': pr.user.login if pr.user else "Unknown",
                        'state': pr.state,
                        'created_at': pr.created_at,
                        'url': pr.html_url
                    })
                except Exception as e:
                    print(f"Error processing PR #{pr.number}: {e}")
                    continue
            
            # Save pull requests to parquet
            if prs_data:
                prs_df = pd.DataFrame(prs_data)
                prs_file = os.path.join(dataset_path, 'pull_requests.parquet')
                prs_df.to_parquet(prs_file, index=False)
                print(f"Saved {len(prs_data)} pull requests to parquet")
            
            print(f"Successfully collected data for {repo_name}")
            return True
            
        except Exception as e:
            print(f"Error collecting data for {repo_name}: {e}")
            return False
    
    def get_available_datasets(self) -> List[str]:
        """Get list of available parquet datasets from datalake"""
        datasets = []
        
        if not os.path.exists(self.datalake_path):
            return datasets
        
        # Walk through all repository folders in datalake
        for repo_folder in os.listdir(self.datalake_path):
            repo_path = os.path.join(self.datalake_path, repo_folder)
            
            if os.path.isdir(repo_path):
                # Look for dataset folders (timestamped folders containing parquet files)
                for item in os.listdir(repo_path):
                    item_path = os.path.join(repo_path, item)
                    if os.path.isdir(item_path):
                        # Check if it contains parquet files
                        has_parquet = any(f.endswith('.parquet') for f in os.listdir(item_path))
                        if has_parquet:
                            # Return relative path from datalake
                            datasets.append(os.path.join(repo_folder, item))
        
        # Sort by modification time (newest first)
        datasets.sort(key=lambda x: os.path.getmtime(os.path.join(self.datalake_path, x)), reverse=True)
        
        return datasets
    
    def get_datasets_for_repo(self, repo_name: str) -> List[str]:
        """Get available datasets for a specific repository"""
        snake_repo_name = self._repo_name_to_snake_case(repo_name)
        all_datasets = self.get_available_datasets()
        
        # Filter datasets that belong to this repository
        repo_datasets = [ds for ds in all_datasets if ds.startswith(snake_repo_name + '/')]
        
        return repo_datasets
    
    def collect_and_create_snapshot(self, repo_name: str, progress_callback: Optional[Callable[[str], None]] = None, quarter: str = "2025-1B") -> Optional[str]:
        """
        Collect repository data and create a Parquet snapshot in one operation
        
        Args:
            repo_name: Name of the repository
            progress_callback: Optional callback function to report progress
            quarter: Quarter identifier (e.g., "2025-1B")
            
        Returns:
            str: Snapshot ID if successful, None if failed
        """
        try:
            if progress_callback:
                progress_callback(f"🔍 Starting data collection for {repo_name}...")
            
            # Get GitHub repository
            repo = self.github.get_repo(repo_name)
            
            # Collect commits
            if progress_callback:
                progress_callback(f"📝 Collecting commits for {repo_name}...")
            
            commits_data = []
            for commit in repo.get_commits():
                try:
                    commits_data.append({
                        'sha': commit.sha,
                        'message': commit.commit.message,
                        'author': commit.commit.author.name if commit.commit.author else "Unknown",
                        'date': commit.commit.author.date.isoformat() if commit.commit.author else datetime.now().isoformat(),
                        'url': commit.html_url
                    })
                except Exception as e:
                    print(f"Error processing commit {commit.sha}: {e}")
                    continue
            
            if progress_callback:
                progress_callback(f"✅ Collected {len(commits_data)} commits")
            
            # Collect pull requests
            if progress_callback:
                progress_callback(f"🔀 Collecting pull requests for {repo_name}...")
            
            prs_data = []
            for pr in repo.get_pulls(state='all'):
                try:
                    prs_data.append({
                        'number': pr.number,
                        'title': pr.title,
                        'author': pr.user.login if pr.user else "Unknown",
                        'state': pr.state,
                        'created_at': pr.created_at.isoformat(),
                        'url': pr.html_url
                    })
                except Exception as e:
                    print(f"Error processing PR #{pr.number}: {e}")
                    continue
            
            if progress_callback:
                progress_callback(f"✅ Collected {len(prs_data)} pull requests")
            
            # Create snapshot
            if progress_callback:
                progress_callback(f"📸 Creating Parquet snapshot...")
            
            snapshot_id = self.snapshot_manager.create_repository_snapshot(repo_name, commits_data, prs_data, quarter)
            
            if snapshot_id:
                if progress_callback:
                    progress_callback(f"✅ Snapshot created successfully: {snapshot_id}")
                return snapshot_id
            else:
                if progress_callback:
                    progress_callback(f"❌ Failed to create snapshot")
                return None
                
        except Exception as e:
            error_msg = f"❌ Error collecting data for {repo_name}: {e}"
            if progress_callback:
                progress_callback(error_msg)
            print(error_msg)
            return None
    
    def load_local_parquet_data(self, dataset_path: str, data_type: str) -> Optional[pd.DataFrame]:
        """
        Load data from local parquet files
        
        Args:
            dataset_path: Path to the dataset folder
            data_type: Type of data ('commits' or 'pull_requests')
            
        Returns:
            DataFrame with the data or None if not found
        """
        try:
            file_path = os.path.join(self.datalake_path, dataset_path, f"{data_type}.parquet")
            
            if os.path.exists(file_path):
                return pd.read_parquet(file_path)
            else:
                return None
                
        except Exception as e:
            print(f"Error loading {data_type} data from {dataset_path}: {e}")
            return None