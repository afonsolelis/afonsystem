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
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.repo_names = self._get_repo_names()
        self._github = None
        self.snapshot_manager = SnapshotManager()
    
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
        snake_name = repo_name.replace('/', '_')
        snake_name = snake_name.replace('-', '_')
        snake_name = re.sub(r'[^a-zA-Z0-9_]', '', snake_name)
        return snake_name.lower()
    
    def get_available_repos(self) -> List[str]:
        """Get list of available repositories from environment"""
        return self.repo_names
    
    
    
    
    
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
            
            repo = self.github.get_repo(repo_name)
            
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
                    continue
            
            if progress_callback:
                progress_callback(f"✅ Collected {len(commits_data)} commits")
            
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
                    continue
            
            if progress_callback:
                progress_callback(f"✅ Collected {len(prs_data)} pull requests")
            
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
            return None
    
