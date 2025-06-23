import os
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime


class DatabaseHelper:
    def __init__(self, repo_path: str):
        """Initialize with repository path instead of single DB file"""
        self.repo_path = repo_path
        self.commits_file = os.path.join(repo_path, 'commits.parquet')
        self.prs_file = os.path.join(repo_path, 'pull_requests.parquet')
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a parquet file exists for the given table"""
        try:
            if table_name == 'commits':
                return os.path.exists(self.commits_file)
            elif table_name == 'pull_requests':
                return os.path.exists(self.prs_file)
            return False
        except Exception:
            return False
    
    def get_table_info(self) -> Dict[str, bool]:
        """Get information about available parquet files"""
        return {
            'has_commits': self.check_table_exists('commits'),
            'has_prs': self.check_table_exists('pull_requests')
        }
    
    def read_commits(self) -> Optional[pd.DataFrame]:
        """Read commits from parquet file"""
        try:
            if self.check_table_exists('commits'):
                return pd.read_parquet(self.commits_file)
            return None
        except Exception as e:
            return None
    
    def read_prs(self) -> Optional[pd.DataFrame]:
        """Read pull requests from parquet file"""
        try:
            if self.check_table_exists('pull_requests'):
                return pd.read_parquet(self.prs_file)
            return None
        except Exception as e:
            return None
    
    def save_commits(self, df: pd.DataFrame):
        """Save commits dataframe to parquet file"""
        try:
            os.makedirs(self.repo_path, exist_ok=True)
            df.to_parquet(self.commits_file, index=False)
        except Exception as e:
            pass
    
    def save_prs(self, df: pd.DataFrame):
        """Save pull requests dataframe to parquet file"""
        try:
            os.makedirs(self.repo_path, exist_ok=True)
            df.to_parquet(self.prs_file, index=False)
        except Exception as e:
            pass
    
    def close(self):
        """No connection to close for parquet files"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()