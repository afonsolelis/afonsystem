from typing import Dict, List, Optional, Tuple
from datetime import date
from repositories.commit_repository import CommitRepository
from repositories.pull_request_repository import PullRequestRepository


class AnalyticsService:
    def __init__(self, dataset_path: str):
        self.commit_repo = CommitRepository(dataset_path)
        self.pr_repo = PullRequestRepository(dataset_path)
    
    def get_commit_kpis(self, start_date: date, end_date: date) -> Dict[str, int]:
        """Get commit KPIs for a date range"""
        return {
            'total_commits': self.commit_repo.count_total_commits(start_date, end_date),
            'feat_commits': self.commit_repo.count_commits_by_type('feat', start_date, end_date),
            'fix_commits': self.commit_repo.count_commits_by_type('fix', start_date, end_date),
            'docs_commits': self.commit_repo.count_commits_by_type('docs', start_date, end_date),
            'chore_commits': self.commit_repo.count_commits_by_type('chore', start_date, end_date),
            'refactor_commits': self.commit_repo.count_commits_by_type('refactor', start_date, end_date),
            'test_commits': self.commit_repo.count_commits_by_type('test', start_date, end_date)
        }
    
    def get_commits_data(self, start_date: date, end_date: date) -> List[Dict]:
        """Get commits data for a date range"""
        return self.commit_repo.get_commits_by_date_range(start_date, end_date)
    
    def get_pull_requests_data(self, start_date: date, end_date: date) -> List[Dict]:
        """Get pull requests data for a date range"""
        return self.pr_repo.get_pull_requests_by_date_range(start_date, end_date)
    
    def get_commits_by_author_chart_data(self, start_date: date, end_date: date) -> List[Dict]:
        """Get commits by author for chart visualization"""
        return self.commit_repo.get_commits_by_author_count(start_date, end_date)
    
    def get_commits_by_type_chart_data(self, start_date: date, end_date: date) -> List[Dict]:
        """Get commits by type for chart visualization"""
        return self.commit_repo.get_commits_by_type_count(start_date, end_date)
    
    def get_daily_commits_chart_data(self, start_date: date, end_date: date) -> List[Dict]:
        """Get daily commits for timeline chart"""
        return self.commit_repo.get_daily_commits_count(start_date, end_date)
    
    def get_date_range(self) -> Optional[Tuple[date, date]]:
        """Get the date range of available commit data"""
        try:
            result = self.commit_repo.get_date_range()
            if result and result[0] and result[1]:
                return result
            return None
        except:
            return None
    
    def close(self):
        """Close repository connections"""
        self.commit_repo.close()
        self.pr_repo.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()