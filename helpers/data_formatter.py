import pandas as pd
from datetime import datetime
from typing import List, Dict, Any


class DataFormatter:
    @staticmethod
    def format_commits_for_display(commits: List[Dict]) -> pd.DataFrame:
        if not commits:
            return pd.DataFrame()
        
        if commits and isinstance(commits[0], tuple):
            commits = [
                {'sha': row[0], 'message': row[1], 'author': row[2], 'date': row[3], 'url': row[4]}
                for row in commits
            ]
        
        df = pd.DataFrame(commits)
        
        if df.empty:
            return pd.DataFrame()
        
        display_columns = ['sha', 'message', 'author', 'date']
        
        available_columns = [col for col in display_columns if col in df.columns]
        
        if 'url' in df.columns:
            df['Link'] = df['url'].apply(lambda x: f"[🔗]({x})")
            available_columns.append('Link')
        
        return df[available_columns] if available_columns else pd.DataFrame()
    
    @staticmethod
    def format_pull_requests_for_display(prs: List[Dict]) -> pd.DataFrame:
        if not prs:
            return pd.DataFrame()
        
        if prs and isinstance(prs[0], tuple):
            prs = [
                {'number': row[0], 'title': row[1], 'author': row[2], 'state': row[3], 'created_at': row[4], 'url': row[5]}
                for row in prs
            ]
        
        df = pd.DataFrame(prs)
        
        if df.empty:
            return pd.DataFrame()
        
        display_columns = ['number', 'title', 'author', 'state', 'created_at']
        
        available_columns = [col for col in display_columns if col in df.columns]
        
        if 'url' in df.columns:
            df['Link'] = df['url'].apply(lambda x: f"[🔗]({x})")
            available_columns.append('Link')
        
        return df[available_columns] if available_columns else pd.DataFrame()
    
    @staticmethod
    def format_timestamp_for_display(timestamp: int) -> str:
        try:
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return "Invalid timestamp"
    
    @staticmethod
    def format_database_options(databases: List[str]) -> List[str]:
        options = []
        for db in databases:
            filename = db.split('/')[-1] if '/' in db else db
            if '_' in filename:
                timestamp_str = filename.split('_')[0]
                try:
                    timestamp = int(timestamp_str)
                    readable_date = DataFormatter.format_timestamp_for_display(timestamp)
                    options.append(f"{readable_date} ({filename})")
                except:
                    options.append(filename)
            else:
                options.append(filename)
        return options