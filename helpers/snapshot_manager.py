import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from supabase import create_client, Client
from dotenv import load_dotenv
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

class SnapshotManager:
    """Manager for creating and handling repository snapshots with Parquet format"""
    
    def __init__(self):
        import time
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager.__init__ start")
        
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_ANON_KEY')
        self.bucket_name = 'afonsystem'
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager config loaded")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        print(f"[DEBUG] {time.time():.2f} - Creating Supabase client in SnapshotManager")
        start_client = time.time()
        self.client: Client = create_client(self.url, self.key)
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager Supabase client created in {time.time() - start_client:.2f}s")
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager.__init__ complete")
    
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
            # Create timestamp for unique naming
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            # Convert repo name to safe format (replace / and - with _)
            safe_repo_name = repo_name.replace('/', '_').replace('-', '_')
            snapshot_id = f"snapshot_{safe_repo_name}_{timestamp}"
            
            # Create quarter-based path structure
            quarter_path = f"{quarter}/{snapshot_id}"
            
            # Create metadata
            metadata = {
                'timestamp': timestamp,
                'repository_name': repo_name,
                'commits_count': len(commits_data),
                'pull_requests_count': len(prs_data) if prs_data else 0,
                'snapshot_id': snapshot_id,
                'created_at': datetime.now().isoformat()
            }
            
            # Create temporary directory for files
            with tempfile.TemporaryDirectory() as temp_dir:
                # Save commits as Parquet
                if commits_data:
                    commits_df = pd.DataFrame(commits_data)
                    commits_path = os.path.join(temp_dir, 'commits.parquet')
                    commits_df.to_parquet(commits_path, index=False)
                    
                    # Upload commits parquet
                    self._upload_file(commits_path, f"{quarter_path}/commits.parquet")
                
                # Save pull requests as Parquet if available
                if prs_data:
                    prs_df = pd.DataFrame(prs_data)
                    prs_path = os.path.join(temp_dir, 'pull_requests.parquet')
                    prs_df.to_parquet(prs_path, index=False)
                    
                    # Upload PRs parquet
                    self._upload_file(prs_path, f"{quarter_path}/pull_requests.parquet")
                
                # Save metadata as JSON
                metadata_path = os.path.join(temp_dir, 'metadata.json')
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                # Upload metadata
                self._upload_file(metadata_path, f"{quarter_path}/metadata.json")
            
            return snapshot_id
            
        except Exception as e:
            raise Exception(f"Error creating snapshot for {repo_name}: {str(e)}")
    
    def _upload_file(self, local_path: str, remote_path: str):
        """Upload a file to Supabase storage"""
        with open(local_path, 'rb') as f:
            file_data = f.read()
        
        result = self.client.storage.from_(self.bucket_name).upload(
            path=remote_path,
            file=file_data,
            file_options={"content-type": self._get_content_type(local_path)}
        )
        
        # Check for errors in Supabase response
        if hasattr(result, 'error') and result.error:
            raise Exception(f"Failed to upload {remote_path}: {result.error}")
        elif not result:
            raise Exception(f"Failed to upload {remote_path}: Upload returned None")
    
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
            import time
            print(f"[DEBUG] {time.time():.2f} - Listing snapshots for repo: {repo_name} in quarter: {quarter}")
            
            # List items in the specific quarter folder
            result = self.client.storage.from_(self.bucket_name).list(quarter)
            print(f"[DEBUG] {time.time():.2f} - Found {len(result) if result else 0} items in quarter folder: {quarter}")
            
            # Handle empty quarter folder
            if not result:
                print(f"[DEBUG] {time.time():.2f} - Quarter folder is empty")
                return []
            
            # Convert repo name to match snapshot format (replace / with _)
            safe_repo_name = repo_name.replace('/', '_').replace('-', '_')
            print(f"[DEBUG] {time.time():.2f} - Looking for snapshots with prefix: snapshot_{safe_repo_name}_")
            
            snapshots = []
            for item in result:
                print(f"[DEBUG] {time.time():.2f} - Checking item: {item['name']}")
                if item['name'].startswith(f'snapshot_{safe_repo_name}_'):
                    print(f"[DEBUG] {time.time():.2f} - Found matching snapshot: {item['name']}")
                    # Try to get metadata for this snapshot
                    try:
                        metadata = self.get_snapshot_metadata(item['name'], quarter)
                        if metadata:
                            snapshots.append(metadata)
                            print(f"[DEBUG] {time.time():.2f} - Added snapshot with metadata")
                    except Exception as e:
                        print(f"[DEBUG] {time.time():.2f} - Failed to get metadata: {e}")
                        # If metadata not found, create basic info from folder name
                        try:
                            parts = item['name'].split('_')
                            if len(parts) >= 3:
                                timestamp = '_'.join(parts[-2:])  # Get last two parts for timestamp
                            else:
                                timestamp = 'unknown'
                            
                            snapshots.append({
                                'snapshot_id': item['name'],
                                'repository_name': repo_name,
                                'timestamp': timestamp,
                                'created_at': item.get('created_at', ''),
                                'commits_count': 0,
                                'pull_requests_count': 0
                            })
                        except:
                            # Skip malformed entries
                            continue
            
            # Sort by timestamp (most recent first)
            snapshots.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return snapshots
            
        except Exception as e:
            # Return empty list instead of raising exception for initialization
            print(f"Warning: Could not list snapshots for {repo_name}: {str(e)}")
            return []
    
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
            # Download metadata file
            result = self.client.storage.from_(self.bucket_name).download(f"{quarter}/{snapshot_id}/metadata.json")
            
            if result:
                metadata = json.loads(result.decode('utf-8'))
                return metadata
            
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
            file_path = f"{quarter}/{snapshot_id}/{data_type}.parquet"
            
            # Download parquet file
            result = self.client.storage.from_(self.bucket_name).download(file_path)
            
            if result:
                # Create temporary file to load parquet
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                    temp_file.write(result)
                    temp_file_path = temp_file.name
                
                try:
                    # Load parquet file
                    df = pd.read_parquet(temp_file_path)
                    return df
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
            
            return None
            
        except Exception as e:
            raise Exception(f"Error loading {data_type} data from snapshot {snapshot_id}: {str(e)}")
    
    def delete_snapshot(self, snapshot_id: str) -> bool:
        """
        Delete a complete snapshot (all files)
        
        Args:
            snapshot_id: ID of the snapshot to delete
            
        Returns:
            bool: True if deletion was successful
        """
        try:
            # List all files in the snapshot folder
            files_to_delete = [
                f"{snapshot_id}/commits.parquet",
                f"{snapshot_id}/pull_requests.parquet", 
                f"{snapshot_id}/metadata.json"
            ]
            
            # Try to delete each file (some might not exist)
            for file_path in files_to_delete:
                try:
                    self.client.storage.from_(self.bucket_name).remove([file_path])
                except:
                    # Ignore if file doesn't exist
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
        file_path = f"{snapshot_id}/{file_type}.{file_extension}"
        
        return self.client.storage.from_(self.bucket_name).get_public_url(file_path)
    
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
            # Return default structure instead of raising exception
            print(f"Warning: Could not export summary for {repo_name}: {str(e)}")
            return {
                'repository_name': repo_name,
                'total_snapshots': 0,
                'latest_snapshot': None,
                'total_commits': 0,
                'total_pull_requests': 0,
                'snapshots': []
            }