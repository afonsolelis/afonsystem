import duckdb
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime


class DatabaseHelper:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            tables = self.conn.execute("SHOW TABLES").fetchdf()
            return table_name in tables['name'].values if not tables.empty else False
        except Exception:
            return False
    
    def get_table_info(self) -> Dict[str, bool]:
        """Get information about available tables"""
        return {
            'has_commits': self.check_table_exists('commits'),
            'has_prs': self.check_table_exists('pull_requests')
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()