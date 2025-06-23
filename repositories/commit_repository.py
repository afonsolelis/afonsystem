import pandas as pd
import os
from typing import List, Optional
from datetime import date


class CommitRepository:
    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.commits_file = os.path.join(dataset_path, 'commits.parquet')
    
    def _read_commits(self) -> pd.DataFrame:
        """Read commits from parquet file"""
        if os.path.exists(self.commits_file):
            return pd.read_parquet(self.commits_file)
        return pd.DataFrame(columns=['sha', 'message', 'author', 'date', 'url'])
    
    def _save_commits(self, df: pd.DataFrame):
        """Save commits to parquet file"""
        os.makedirs(self.dataset_path, exist_ok=True)
        df.to_parquet(self.commits_file, index=False)
    
    def get_commits_by_date_range(self, start_date: date, end_date: date) -> List[dict]:
        """Get commits within date range"""
        df = self._read_commits()
        if df.empty:
            return []
        
        # Convert date column to datetime if it's not already
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter by date range
        mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
        filtered_df = df[mask].sort_values('date', ascending=False)
        
        return filtered_df.to_dict('records')
    
    def get_commits_by_author(self, author: str) -> List[dict]:
        """Get commits by author"""
        df = self._read_commits()
        if df.empty:
            return []
        
        filtered_df = df[df['author'] == author].sort_values('date', ascending=False)
        return filtered_df.to_dict('records')
    
    def count_total_commits(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count total commits, optionally within date range"""
        df = self._read_commits()
        if df.empty:
            return 0
        
        if start_date and end_date:
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
            return len(df[mask])
        
        return len(df)
    
    def count_commits_by_type(self, type_prefix: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count commits by type prefix"""
        df = self._read_commits()
        if df.empty:
            return 0
        
        # Filter by date range if provided
        if start_date and end_date:
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
            df = df[mask]
        
        # Filter by type prefix
        type_mask = df['message'].str.lower().str.startswith(type_prefix.lower())
        return len(df[type_mask])
    
    def get_commits_by_author_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get commit counts grouped by author"""
        df = self._read_commits()
        if df.empty:
            return []
        
        # Filter by date range if provided
        if start_date and end_date:
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
            df = df[mask]
        
        # Group by author and count
        author_counts = df.groupby('author').size().reset_index(name='count')
        author_counts = author_counts.sort_values('count', ascending=False)
        
        return author_counts.to_dict('records')
    
    def get_commits_by_type_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get commit counts grouped by type"""
        df = self._read_commits()
        if df.empty:
            return []
        
        # Filter by date range if provided
        if start_date and end_date:
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
            df = df[mask]
        
        # Categorize commits by type
        def categorize_commit(message):
            message_lower = message.lower()
            if message_lower.startswith('feat'):
                return 'feat'
            elif message_lower.startswith('fix'):
                return 'fix'
            elif message_lower.startswith('docs'):
                return 'docs'
            elif message_lower.startswith('chore'):
                return 'chore'
            elif message_lower.startswith('refactor'):
                return 'refactor'
            elif message_lower.startswith('test'):
                return 'test'
            elif message_lower.startswith('merge'):
                return 'merge'
            else:
                return 'other'
        
        df['commit_type'] = df['message'].apply(categorize_commit)
        type_counts = df.groupby('commit_type').size().reset_index(name='count')
        type_counts = type_counts.sort_values('count', ascending=False)
        
        return type_counts.to_dict('records')
    
    def get_daily_commits_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get daily commit counts"""
        df = self._read_commits()
        if df.empty:
            return []
        
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter by date range if provided
        if start_date and end_date:
            mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
            df = df[mask]
        
        # Group by date
        df['day'] = df['date'].dt.date
        daily_counts = df.groupby('day').size().reset_index(name='count')
        daily_counts = daily_counts.sort_values('day')
        
        # Convert day back to string for JSON serialization
        daily_counts['day'] = daily_counts['day'].astype(str)
        
        return daily_counts.to_dict('records')
    
    def get_date_range(self) -> tuple:
        """Get min and max dates from commits"""
        df = self._read_commits()
        if df.empty:
            return (None, None)
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna(subset=['date'])
        
        if df.empty:
            return (None, None)
        
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        return (min_date, max_date)
    
    def close(self):
        """No connection to close for parquet files"""
        pass