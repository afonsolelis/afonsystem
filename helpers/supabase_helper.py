import os
import pandas as pd
from typing import List, Dict, Optional
from dotenv import load_dotenv
from .snapshot_manager import SnapshotManager

load_dotenv()

class SupabaseHelper:
    """Compatibility wrapper that delegates to local SnapshotManager (Supabase removed)."""
    def __init__(self):
        self.snapshot_manager = SnapshotManager()

    def create_parquet_snapshot(self, repo_name: str, commits_data: List[Dict], prs_data: List[Dict] = None, quarter: str = "2025-1B") -> str:
        return self.snapshot_manager.create_repository_snapshot(repo_name, commits_data, prs_data, quarter)

    def list_parquet_snapshots(self, repo_name: str, quarter: str = "2025-1B") -> List[Dict]:
        return self.snapshot_manager.list_repository_snapshots(repo_name, quarter)

    def load_snapshot_data(self, snapshot_id: str, data_type: str, quarter: str = "2025-1B") -> Optional[pd.DataFrame]:
        return self.snapshot_manager.load_snapshot_data(snapshot_id, data_type, quarter)

    def delete_parquet_snapshot(self, snapshot_id: str, quarter: str = None) -> bool:
        return self.snapshot_manager.delete_snapshot(snapshot_id, quarter)

    def get_snapshot_summary(self, repo_name: str) -> Dict:
        return self.snapshot_manager.export_snapshot_summary(repo_name)
