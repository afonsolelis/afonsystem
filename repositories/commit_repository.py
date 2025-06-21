import duckdb
from typing import List, Optional
from datetime import date
from models.commit import Commit


class CommitRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    def create_table(self):
        """Create commits table if it doesn't exist"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha VARCHAR PRIMARY KEY,
                message TEXT NOT NULL,
                author VARCHAR NOT NULL,
                date TIMESTAMP NOT NULL,
                url VARCHAR NOT NULL
            )
        """)
    
    def insert_commit(self, commit: Commit) -> bool:
        """Insert a single commit"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO commits (sha, message, author, date, url)
                VALUES (?, ?, ?, ?, ?)
            """, [commit.sha, commit.message, commit.author, commit.date, commit.url])
            return True
        except Exception as e:
            print(f"Error inserting commit: {e}")
            return False
    
    def insert_commits(self, commits: List[Commit]) -> bool:
        """Insert multiple commits"""
        try:
            data = [(c.sha, c.message, c.author, c.date, c.url) for c in commits]
            self.conn.executemany("""
                INSERT OR REPLACE INTO commits (sha, message, author, date, url)
                VALUES (?, ?, ?, ?, ?)
            """, data)
            return True
        except Exception as e:
            print(f"Error inserting commits: {e}")
            return False
    
    def get_commits_by_date_range(self, start_date: date, end_date: date) -> List[dict]:
        """Get commits within date range"""
        result = self.conn.execute("""
            SELECT sha, message, author, date, url
            FROM commits
            WHERE date BETWEEN ? AND ?
            ORDER BY date DESC
        """, [start_date, end_date])
        return result.fetchall()
    
    def get_commits_by_author(self, author: str) -> List[dict]:
        """Get commits by author"""
        result = self.conn.execute("""
            SELECT sha, message, author, date, url
            FROM commits
            WHERE author = ?
            ORDER BY date DESC
        """, [author])
        return result.fetchall()
    
    def count_total_commits(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count total commits, optionally within date range"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM commits 
                WHERE date BETWEEN ? AND ?
            """, [start_date, end_date])
        else:
            result = self.conn.execute("SELECT COUNT(*) FROM commits")
        return result.fetchone()[0]
    
    def count_commits_by_type(self, type_prefix: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
        """Count commits by type prefix"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM commits
                WHERE date BETWEEN ? AND ?
                AND LOWER(message) LIKE ?
            """, [start_date, end_date, f"{type_prefix.lower()}%"])
        else:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM commits
                WHERE LOWER(message) LIKE ?
            """, [f"{type_prefix.lower()}%"])
        return result.fetchone()[0]
    
    def get_commits_by_author_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get commit counts grouped by author"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT author, COUNT(*) as count
                FROM commits
                WHERE date BETWEEN ? AND ?
                GROUP BY author
                ORDER BY count DESC
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT author, COUNT(*) as count
                FROM commits
                GROUP BY author
                ORDER BY count DESC
            """)
        return result.fetchall()
    
    def get_commits_by_type_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get commit counts grouped by type"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT 
                    CASE 
                        WHEN LOWER(message) LIKE 'feat%' THEN 'feat'
                        WHEN LOWER(message) LIKE 'fix%' THEN 'fix'
                        WHEN LOWER(message) LIKE 'docs%' THEN 'docs'
                        WHEN LOWER(message) LIKE 'chore%' THEN 'chore'
                        WHEN LOWER(message) LIKE 'refactor%' THEN 'refactor'
                        WHEN LOWER(message) LIKE 'test%' THEN 'test'
                        WHEN LOWER(message) LIKE 'merge%' THEN 'merge'
                        ELSE 'other'
                    END as commit_type,
                    COUNT(*) as count
                FROM commits
                WHERE date BETWEEN ? AND ?
                GROUP BY commit_type
                ORDER BY count DESC
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT 
                    CASE 
                        WHEN LOWER(message) LIKE 'feat%' THEN 'feat'
                        WHEN LOWER(message) LIKE 'fix%' THEN 'fix'
                        WHEN LOWER(message) LIKE 'docs%' THEN 'docs'
                        WHEN LOWER(message) LIKE 'chore%' THEN 'chore'
                        WHEN LOWER(message) LIKE 'refactor%' THEN 'refactor'
                        WHEN LOWER(message) LIKE 'test%' THEN 'test'
                        WHEN LOWER(message) LIKE 'merge%' THEN 'merge'
                        ELSE 'other'
                    END as commit_type,
                    COUNT(*) as count
                FROM commits
                GROUP BY commit_type
                ORDER BY count DESC
            """)
        return result.fetchall()
    
    def get_daily_commits_count(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[dict]:
        """Get daily commit counts"""
        if start_date and end_date:
            result = self.conn.execute("""
                SELECT DATE(date) as day, COUNT(*) as count
                FROM commits
                WHERE date BETWEEN ? AND ?
                GROUP BY DATE(date)
                ORDER BY day
            """, [start_date, end_date])
        else:
            result = self.conn.execute("""
                SELECT DATE(date) as day, COUNT(*) as count
                FROM commits
                GROUP BY DATE(date)
                ORDER BY day
            """)
        return result.fetchall()
    
    def get_date_range(self) -> tuple:
        """Get min and max dates from commits"""
        result = self.conn.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date 
            FROM commits 
            WHERE date IS NOT NULL
        """)
        return result.fetchone()
    
    def close(self):
        """Close database connection"""
        self.conn.close()