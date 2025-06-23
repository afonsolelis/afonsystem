import os
import zipfile
import tempfile
import shutil
import pandas as pd
import json
from datetime import datetime
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
from .snapshot_manager import SnapshotManager

load_dotenv()

class SupabaseHelper:
    """Helper class for managing Supabase storage operations with Parquet support"""
    
    def __init__(self):
        import time
        print(f"[DEBUG] {time.time():.2f} - SupabaseHelper.__init__ start")
        
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_ANON_KEY')
        self.bucket_name = 'afonsystem'
        print(f"[DEBUG] {time.time():.2f} - Supabase config loaded")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        print(f"[DEBUG] {time.time():.2f} - Creating Supabase client")
        start_client = time.time()
        self.client: Client = create_client(self.url, self.key)
        print(f"[DEBUG] {time.time():.2f} - Supabase client created in {time.time() - start_client:.2f}s")
        
        print(f"[DEBUG] {time.time():.2f} - Creating SnapshotManager")
        start_sm = time.time()
        self.snapshot_manager = SnapshotManager()
        print(f"[DEBUG] {time.time():.2f} - SnapshotManager created in {time.time() - start_sm:.2f}s")
        print(f"[DEBUG] {time.time():.2f} - SupabaseHelper.__init__ complete")
    
    def create_repository_snapshot(self, repo_name: str) -> str:
        """
        Create a snapshot of a specific repository and upload to Supabase
        
        Args:
            repo_name: Name of the repository (folder name in datalake)
            
        Returns:
            str: Public URL of the uploaded snapshot or None if failed
        """
        repo_path = f"datalake/{repo_name}"
        
        if not os.path.exists(repo_path):
            raise FileNotFoundError(f"Repository path {repo_path} does not exist")
        
        # Create timestamp for unique naming
        timestamp = int(datetime.now().timestamp())
        snapshot_filename = f"{timestamp}_{repo_name}_snapshot.zip"
        
        # Create temporary zip file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            # Create zip archive of the repository folder
            with zipfile.ZipFile(temp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(repo_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, repo_path)
                        zipf.write(file_path, arcname)
            
            # Upload to Supabase storage
            with open(temp_zip_path, 'rb') as f:
                file_data = f.read()
            
            # Upload file to bucket
            result = self.client.storage.from_(self.bucket_name).upload(
                path=snapshot_filename,
                file=file_data,
                file_options={"content-type": "application/zip"}
            )
            
            # Check for errors in Supabase response
            if hasattr(result, 'error') and result.error:
                raise Exception(f"Failed to upload snapshot: {result.error}")
            elif not result:
                raise Exception(f"Failed to upload snapshot: Upload returned None")
            
            # Get public URL
            public_url = self.client.storage.from_(self.bucket_name).get_public_url(snapshot_filename)
            
            return public_url
            
        except Exception as e:
            raise Exception(f"Error creating snapshot: {str(e)}")
        
        finally:
            # Clean up temporary file
            if os.path.exists(temp_zip_path):
                os.unlink(temp_zip_path)
    
    def create_parquet_snapshot(self, repo_name: str, commits_data: List[Dict], prs_data: List[Dict] = None, quarter: str = "2025-1B") -> str:
        """
        Create a Parquet-based snapshot for a repository using the snapshot manager
        
        Args:
            repo_name: Name of the repository
            commits_data: List of commit dictionaries
            prs_data: Optional list of pull request dictionaries
            quarter: Quarter identifier (e.g., "2025-1B")
            
        Returns:
            str: Snapshot ID if successful
        """
        return self.snapshot_manager.create_repository_snapshot(repo_name, commits_data, prs_data, quarter)
    
    def list_parquet_snapshots(self, repo_name: str, quarter: str = "2025-1B") -> List[Dict]:
        """
        List Parquet snapshots for a repository
        
        Args:
            repo_name: Name of the repository
            quarter: Quarter identifier (e.g., "2025-1B")
            
        Returns:
            List of snapshot metadata dictionaries
        """
        return self.snapshot_manager.list_repository_snapshots(repo_name, quarter)
    
    def load_snapshot_data(self, snapshot_id: str, data_type: str, quarter: str = "2025-1B") -> Optional[pd.DataFrame]:
        """
        Load data from a Parquet snapshot
        
        Args:
            snapshot_id: ID of the snapshot
            data_type: Type of data ('commits' or 'pull_requests')
            quarter: Quarter identifier (e.g., "2025-1B")
            
        Returns:
            DataFrame with the data
        """
        return self.snapshot_manager.load_snapshot_data(snapshot_id, data_type, quarter)
    
    def delete_parquet_snapshot(self, snapshot_id: str) -> bool:
        """
        Delete a Parquet snapshot
        
        Args:
            snapshot_id: ID of the snapshot to delete
            
        Returns:
            bool: True if successful
        """
        return self.snapshot_manager.delete_snapshot(snapshot_id)
    
    def get_snapshot_summary(self, repo_name: str) -> Dict:
        """
        Get summary of all snapshots for a repository
        
        Args:
            repo_name: Name of the repository
            
        Returns:
            Dictionary with summary statistics
        """
        return self.snapshot_manager.export_snapshot_summary(repo_name)
    
    def list_snapshots(self, repo_name: str = None) -> list:
        """
        List all snapshots in the bucket, optionally filtered by repository name
        
        Args:
            repo_name: Optional repository name to filter snapshots
            
        Returns:
            list: List of snapshot files
        """
        try:
            result = self.client.storage.from_(self.bucket_name).list()
            
            if repo_name:
                # Filter snapshots for specific repository
                filtered_snapshots = [
                    file for file in result 
                    if file['name'].endswith('_snapshot.zip') and repo_name in file['name']
                ]
                return filtered_snapshots
            
            return [file for file in result if file['name'].endswith('_snapshot.zip')]
            
        except Exception as e:
            raise Exception(f"Error listing snapshots: {str(e)}")
    
    def get_snapshot_url(self, snapshot_filename: str) -> str:
        """
        Get public URL for a snapshot file
        
        Args:
            snapshot_filename: Name of the snapshot file
            
        Returns:
            str: Public URL of the snapshot
        """
        return self.client.storage.from_(self.bucket_name).get_public_url(snapshot_filename)
    
    def delete_snapshot(self, snapshot_filename: str) -> bool:
        """
        Delete a snapshot file from storage
        
        Args:
            snapshot_filename: Name of the snapshot file to delete
            
        Returns:
            bool: True if deletion was successful
        """
        try:
            result = self.client.storage.from_(self.bucket_name).remove([snapshot_filename])
            # Check for errors in Supabase response
            if hasattr(result, 'error') and result.error:
                return False
            return True
        except Exception as e:
            raise Exception(f"Error deleting snapshot: {str(e)}")