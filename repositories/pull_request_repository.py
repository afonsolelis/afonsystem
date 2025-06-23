import pandas as pd
import os
from typing import List, Optional
from datetime import date


class PullRequestRepository:
    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.prs_file = os.path.join(dataset_path, 'pull_requests.parquet')
    
    def _read_prs(self) -> pd.DataFrame:
        """Read pull requests from parquet file"""
        if os.path.exists(self.prs_file):
            return pd.read_parquet(self.prs_file)
        return pd.DataFrame(columns=['number', 'title', 'author', 'state', 'created_at', 'url'])
    
    def _save_prs(self, df: pd.DataFrame):
        """Save pull requests to parquet file"""
        os.makedirs(self.dataset_path, exist_ok=True)
        df.to_parquet(self.prs_file, index=False)
    
    def get_pull_requests_by_date_range(self, start_date: date, end_date: date) -> List[dict]:
        """Get pull requests within date range"""
        df = self._read_prs()
        if df.empty:
            return []
        
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
        filtered_df = df[mask].sort_values('created_at', ascending=False)
        
        return filtered_df.to_dict('records')
    
    def get_pull_requests_by_author(self, author: str) -> List[dict]:
        """Get pull requests by author"""
        df = self._read_prs()
        if df.empty:
            return []
        
        filtered_df = df[df['author'] == author].sort_values('created_at', ascending=False)
        return filtered_df.to_dict('records')
    
    def get_pull_requests_by_state(self, state: str) -> List[dict]:
        """Get pull requests by state"""
        df = self._read_prs()
        if df.empty:
            return []
        
        filtered_df = df[df['state'] == state].sort_values('created_at', ascending=False)
        return filtered_df.to_dict('records')
    
    def count_total_pull_requests(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count total pull requests, optionally within date range"""
        df = self._read_prs()
        if df.empty:
            return 0
        
        if start_date and end_date:
            df['created_at'] = pd.to_datetime(df['created_at'])
            mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
            return len(df[mask])
        
        return len(df)
    
    def count_pull_requests_by_state(self, state: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count pull requests by state"""
        df = self._read_prs()
        if df.empty:
            return 0
        
        if start_date and end_date:
            df['created_at'] = pd.to_datetime(df['created_at'])
            mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
            df = df[mask]
        
        state_mask = df['state'] == state
        return len(df[state_mask])
    
    def get_pull_requests_by_author_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get pull request counts grouped by author"""
        df = self._read_prs()
        if df.empty:
            return []
        
        if start_date and end_date:
            df['created_at'] = pd.to_datetime(df['created_at'])
            mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
            df = df[mask]
        
        author_counts = df.groupby('author').size().reset_index(name='count')
        author_counts = author_counts.sort_values('count', ascending=False)
        
        return author_counts.to_dict('records')
    
    def get_pull_requests_by_state_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get pull request counts grouped by state"""
        df = self._read_prs()
        if df.empty:
            return []
        
        if start_date and end_date:
            df['created_at'] = pd.to_datetime(df['created_at'])
            mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
            df = df[mask]
        
        state_counts = df.groupby('state').size().reset_index(name='count')
        state_counts = state_counts.sort_values('count', ascending=False)
        
        return state_counts.to_dict('records')
    
    def get_daily_pull_requests_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get daily pull request counts"""
        df = self._read_prs()
        if df.empty:
            return []
        
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        if start_date and end_date:
            mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
            df = df[mask]
        
        df['day'] = df['created_at'].dt.date
        daily_counts = df.groupby('day').size().reset_index(name='count')
        daily_counts = daily_counts.sort_values('day')
        
        daily_counts['day'] = daily_counts['day'].astype(str)
        
        return daily_counts.to_dict('records')
    
    def get_date_range(self) -> tuple:
        """Get min and max dates from pull requests"""
        df = self._read_prs()
        if df.empty:
            return (None, None)
        
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df.dropna(subset=['created_at'])
        
        if df.empty:
            return (None, None)
        
        min_date = df['created_at'].min()
        max_date = df['created_at'].max()
        
        return (min_date, max_date)
    
    def close(self):
        """No connection to close for parquet files"""
        pass