import duckdb
from typing import List, Optional
from datetime import date
from models.pull_request import PullRequest


class PullRequestRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    def create_table(self):
        """Create pull_requests table if it doesn't exist"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pull_requests (
                number INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                author VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                url VARCHAR NOT NULL
            )
        """)
    
    def insert_pull_request(self, pr: PullRequest) -> bool:
        """Insert a single pull request"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO pull_requests (number, title, author, state, created_at, url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [pr.number, pr.title, pr.author, pr.state, pr.created_at, pr.url])
            return True
        except Exception as e:
            print(f"Error inserting pull request: {e}")
            return False
    
    def insert_pull_requests(self, prs: List[PullRequest]) -> bool:
        """Insert multiple pull requests"""
        try:
            data = [(pr.number, pr.title, pr.author, pr.state, pr.created_at, pr.url) for pr in prs]
            self.conn.executemany("""
                INSERT OR REPLACE INTO pull_requests (number, title, author, state, created_at, url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, data)
            return True
        except Exception as e:
            print(f"Error inserting pull requests: {e}")
            return False
    
    def get_pull_requests_by_date_range(self, start_date: date, end_date: date) -> List[dict]:
        """Get pull requests within date range"""
        result = self.conn.execute("""
            SELECT number, title, author, state, created_at, url
            FROM pull_requests
            WHERE created_at BETWEEN ? AND ?
            ORDER BY created_at DESC
        """, [start_date, end_date])
        return result.fetchall()
    
    def get_pull_requests_by_author(self, author: str) -> List[dict]:
        """Get pull requests by author"""
        result = self.conn.execute("""
            SELECT number, title, author, state, created_at, url
            FROM pull_requests
            WHERE author = ?
            ORDER BY created_at DESC
        """, [author])
        return result.fetchall()
    
    def get_pull_requests_by_state(self, state: str) -> List[dict]:
        """Get pull requests by state"""
        result = self.conn.execute("""
            SELECT number, title, author, state, created_at, url
            FROM pull_requests
            WHERE state = ?
            ORDER BY created_at DESC
        """, [state])
        return result.fetchall()
    
    def count_total_pull_requests(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count total pull requests, optionally within date range"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM pull_requests 
                WHERE created_at BETWEEN ? AND ?
            """, [start_date, end_date])
        else:
            result = self.conn.execute("SELECT COUNT(*) FROM pull_requests")
        return result.fetchone()[0]
    
    def count_pull_requests_by_state(self, state: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count pull requests by state"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM pull_requests
                WHERE state = ? AND created_at BETWEEN ? AND ?
            """, [state, start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM pull_requests
                WHERE state = ?
            """, [state])
        return result.fetchone()[0]
    
    def get_pull_requests_by_author_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get pull request counts grouped by author"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT author, COUNT(*) as count
                FROM pull_requests
                WHERE created_at BETWEEN ? AND ?
                GROUP BY author
                ORDER BY count DESC
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT author, COUNT(*) as count
                FROM pull_requests
                GROUP BY author
                ORDER BY count DESC
            """)
        return result.fetchall()
    
    def get_pull_requests_by_state_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get pull request counts grouped by state"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT state, COUNT(*) as count
                FROM pull_requests
                WHERE created_at BETWEEN ? AND ?
                GROUP BY state
                ORDER BY count DESC
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT state, COUNT(*) as count
                FROM pull_requests
                GROUP BY state
                ORDER BY count DESC
            """)
        return result.fetchall()
    
    def get_daily_pull_requests_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get daily pull request counts"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT DATE(created_at) as day, COUNT(*) as count
                FROM pull_requests
                WHERE created_at BETWEEN ? AND ?
                GROUP BY DATE(created_at)
                ORDER BY day
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT DATE(created_at) as day, COUNT(*) as count
                FROM pull_requests
                GROUP BY DATE(created_at)
                ORDER BY day
            """)
        return result.fetchall()
    
    def get_date_range(self) -> tuple:
        """Get min and max dates from pull requests"""
        result = self.conn.execute("""
            SELECT MIN(created_at) as min_date, MAX(created_at) as max_date 
            FROM pull_requests 
            WHERE created_at IS NOT NULL
        """)
        return result.fetchone()
    
    def close(self):
        """Close database connection"""
        self.conn.close()