import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv
import tempfile

load_dotenv()

class SnapshotManager:
    """Manager for creating and handling repository snapshots with Parquet format (local filesystem)."""

    def __init__(self, base_path: Optional[str] = None):
        # Base path for storing snapshots locally
        self.base_path = base_path or os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.base_path, exist_ok=True)

    def create_repository_snapshot(self, repo_name: str, commits_data: List[Dict], prs_data: List[Dict] = None, quarter: str = "2025-1B") -> str:
        """
        Create a Parquet-based snapshot for a specific repository

        Args:
            repo_name: Name of the repository
            commits_data: List of commit dictionaries
            prs_data: Optional list of pull request dictionaries
            quarter: Quarter identifier (e.g., "2025-1B")

        Returns:
            str: Snapshot ID if successful, None if failed
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            safe_repo_name = repo_name.replace('/', '_').replace('-', '_')
            snapshot_id = f"snapshot_{safe_repo_name}_{timestamp}"

            quarter_path = os.path.join(self.base_path, quarter, snapshot_id)
            os.makedirs(quarter_path, exist_ok=True)

            metadata = {
                'timestamp': timestamp,
                'repository_name': repo_name,
                'commits_count': len(commits_data),
                'pull_requests_count': len(prs_data) if prs_data else 0,
                'snapshot_id': snapshot_id,
                'created_at': datetime.now().isoformat()
            }

            # Write Parquet and metadata locally
            if commits_data:
                commits_df = pd.DataFrame(commits_data)
                commits_path = os.path.join(quarter_path, 'commits.parquet')
                commits_df.to_parquet(commits_path, index=False)

            if prs_data:
                prs_df = pd.DataFrame(prs_data)
                prs_path = os.path.join(quarter_path, 'pull_requests.parquet')
                prs_df.to_parquet(prs_path, index=False)

            metadata_path = os.path.join(quarter_path, 'metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            return snapshot_id

        except Exception as e:
            raise Exception(f"Error creating snapshot for {repo_name}: {str(e)}")

    # No upload in local mode
    def _get_content_type(self, file_path: str) -> str:
        return 'application/octet-stream'

    def _get_content_type(self, file_path: str) -> str:
        """Get content type based on file extension"""
        if file_path.endswith('.parquet'):
            return 'application/octet-stream'
        elif file_path.endswith('.json'):
            return 'application/json'
        else:
            return 'application/octet-stream'

    def list_repository_snapshots(self, repo_name: str, quarter: str = "2025-1B") -> List[Dict]:
        """
        List all snapshots for a specific repository in a specific quarter

        Args:
            repo_name: Name of the repository
            quarter: Quarter identifier (e.g., "2025-1B")

        Returns:
            List of snapshot metadata dictionaries
        """
        try:
            quarter_dir = os.path.join(self.base_path, quarter)
            if not os.path.isdir(quarter_dir):
                return []

            snapshots: List[Dict] = []
            safe_repo_name_current = repo_name.replace('/', '_').replace('-', '_')
            safe_repo_name_lower = safe_repo_name_current.lower()

            for item_name in os.listdir(quarter_dir):
                if not item_name.startswith('snapshot_'):
                    continue
                matches_repo_prefix = (
                    item_name.startswith(f'snapshot_{safe_repo_name_current}_') or
                    item_name.startswith(f'snapshot_{safe_repo_name_lower}_')
                )
                if not matches_repo_prefix:
                    continue
                metadata = self.get_snapshot_metadata(item_name, quarter)
                if metadata and metadata.get('repository_name') == repo_name:
                    snapshots.append(metadata)
                    continue
                parts = item_name.split('_')
                timestamp = 'unknown'
                if len(parts) >= 3:
                    timestamp = '_'.join(parts[-2:])
                snapshots.append({
                    'snapshot_id': item_name,
                    'repository_name': repo_name,
                    'timestamp': timestamp,
                    'created_at': '',
                    'commits_count': 0,
                    'pull_requests_count': 0
                })
            snapshots.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return snapshots

        except Exception:
            return []

    # No remote listing in local mode

    def get_snapshot_metadata(self, snapshot_id: str, quarter: str = "2025-1B") -> Optional[Dict]:
        """
        Get metadata for a specific snapshot

        Args:
            snapshot_id: ID of the snapshot
            quarter: Quarter identifier (e.g., "2025-1B")

        Returns:
            Dictionary with snapshot metadata or None if not found
        """
        try:
            metadata_path = os.path.join(self.base_path, quarter, snapshot_id, 'metadata.json')
            if os.path.isfile(metadata_path):
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            return None

        except Exception:
            return None

    def load_snapshot_data(self, snapshot_id: str, data_type: str, quarter: str = "2025-1B") -> Optional[pd.DataFrame]:
        """
        Load data from a snapshot

        Args:
            snapshot_id: ID of the snapshot
            data_type: Type of data to load ('commits' or 'pull_requests')
            quarter: Quarter identifier (e.g., "2025-1B")

        Returns:
            pandas DataFrame with the data or None if not found
        """
        try:
            file_path = os.path.join(self.base_path, quarter, snapshot_id, f"{data_type}.parquet")
            if os.path.isfile(file_path):
                df = pd.read_parquet(file_path)
                return df
            return None

        except Exception as e:
            raise Exception(f"Error loading {data_type} data from snapshot {snapshot_id}: {str(e)}")

    def delete_snapshot(self, snapshot_id: str, quarter: Optional[str] = None) -> bool:
        """
        Delete a complete snapshot (all files)

        Args:
            snapshot_id: ID of the snapshot to delete

        Returns:
            bool: True if deletion was successful
        """
        try:
            if not quarter:
                return False
            snapshot_dir = os.path.join(self.base_path, quarter, snapshot_id)
            if os.path.isdir(snapshot_dir):
                for root, dirs, files in os.walk(snapshot_dir, topdown=False):
                    for name in files:
                        try:
                            os.remove(os.path.join(root, name))
                        except Exception:
                            pass
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except Exception:
                            pass
                try:
                    os.rmdir(snapshot_dir)
                except Exception:
                    pass
            return True

        except Exception as e:
            raise Exception(f"Error deleting snapshot {snapshot_id}: {str(e)}")

    def get_snapshot_url(self, snapshot_id: str, file_type: str) -> str:
        """
        Get public URL for a snapshot file

        Args:
            snapshot_id: ID of the snapshot
            file_type: Type of file ('commits', 'pull_requests', or 'metadata')

        Returns:
            str: Public URL of the file
        """
        file_extension = 'parquet' if file_type != 'metadata' else 'json'
        return os.path.join(self.base_path, f"{snapshot_id}/{file_type}.{file_extension}")

    def export_snapshot_summary(self, repo_name: str) -> Dict:
        """
        Export a summary of all snapshots for a repository

        Args:
            repo_name: Name of the repository

        Returns:
            Dictionary with snapshot summary statistics
        """
        try:
            snapshots = self.list_repository_snapshots(repo_name)

            if not snapshots:
                return {
                    'repository_name': repo_name,
                    'total_snapshots': 0,
                    'latest_snapshot': None,
                    'total_commits': 0,
                    'total_pull_requests': 0,
                    'snapshots': []
                }

            total_commits = sum(s.get('commits_count', 0) for s in snapshots)
            total_prs = sum(s.get('pull_requests_count', 0) for s in snapshots)

            return {
                'repository_name': repo_name,
                'total_snapshots': len(snapshots),
                'latest_snapshot': snapshots[0] if snapshots else None,
                'total_commits': total_commits,
                'total_pull_requests': total_prs,
                'snapshots': snapshots
            }

        except Exception as e:
            return {
                'repository_name': repo_name,
                'total_snapshots': 0,
                'latest_snapshot': None,
                'total_commits': 0,
                'total_pull_requests': 0,
                'snapshots': []
            }
