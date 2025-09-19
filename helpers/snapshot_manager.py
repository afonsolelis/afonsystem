import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from supabase import create_client, Client
from dotenv import load_dotenv
import tempfile

load_dotenv()

class SnapshotManager:
    """Manager for creating and handling repository snapshots with Parquet format"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_ANON_KEY')
        self.bucket_name = 'afonsystem'
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.url, self.key)
    
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
            
            quarter_path = f"{quarter}/{snapshot_id}"
            
            metadata = {
                'timestamp': timestamp,
                'repository_name': repo_name,
                'commits_count': len(commits_data),
                'pull_requests_count': len(prs_data) if prs_data else 0,
                'snapshot_id': snapshot_id,
                'created_at': datetime.now().isoformat()
            }
            
            with tempfile.TemporaryDirectory() as temp_dir:
                if commits_data:
                    commits_df = pd.DataFrame(commits_data)
                    commits_path = os.path.join(temp_dir, 'commits.parquet')
                    commits_df.to_parquet(commits_path, index=False)
                    
                    self._upload_file(commits_path, f"{quarter_path}/commits.parquet")
                
                if prs_data:
                    prs_df = pd.DataFrame(prs_data)
                    prs_path = os.path.join(temp_dir, 'pull_requests.parquet')
                    prs_df.to_parquet(prs_path, index=False)
                    
                    self._upload_file(prs_path, f"{quarter_path}/pull_requests.parquet")
                
                metadata_path = os.path.join(temp_dir, 'metadata.json')
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
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
            # Paginação para listar todo o conteúdo do trimestre
            result = self._list_all(quarter)

            if not result:
                return []

            snapshots: List[Dict] = []

            # Heurísticas de compatibilidade: nomes possíveis de diretório por variações antigas
            safe_repo_name_current = repo_name.replace('/', '_').replace('-', '_')
            safe_repo_name_lower = safe_repo_name_current.lower()

            for item in result:
                item_name = item.get('name') or ''

                # Considera apenas diretórios de snapshot
                if not item_name.startswith('snapshot_'):
                    continue

                # Pré-filtra por prefixo do repositório para evitar downloads desnecessários de metadata
                matches_repo_prefix = (
                    item_name.startswith(f'snapshot_{safe_repo_name_current}_') or
                    item_name.startswith(f'snapshot_{safe_repo_name_lower}_')
                )
                if not matches_repo_prefix:
                    # Ignora snapshots de outros repositórios sem tentar baixar metadata
                    continue

                # Tenta ler metadata para enriquecer (contagens etc.)
                metadata = self.get_snapshot_metadata(item_name, quarter)

                if metadata and metadata.get('repository_name'):
                    if metadata['repository_name'] == repo_name:
                        snapshots.append(metadata)
                    # Se metadata existe mas é de outro repo (nome coincidente), ignora
                    continue

                # Fallback por nome quando não há metadata
                parts = item_name.split('_')
                timestamp = 'unknown'
                if len(parts) >= 3:
                    timestamp = '_'.join(parts[-2:])

                snapshots.append({
                    'snapshot_id': item_name,
                    'repository_name': repo_name,
                    'timestamp': timestamp,
                    'created_at': item.get('created_at', ''),
                    'commits_count': 0,
                    'pull_requests_count': 0
                })

            snapshots.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return snapshots

        except Exception:
            return []

    def _list_all(self, path: str) -> List[Dict]:
        """List all items under a path using pagination (limit/offset)."""
        items: List[Dict] = []
        limit = 100
        offset = 0
        while True:
            batch = self.client.storage.from_(self.bucket_name).list(path, {
                'limit': limit,
                'offset': offset,
                'sortBy': {'column': 'name', 'order': 'asc'}
            })
            if not batch:
                break
            items.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return items
    
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
            
            result = self.client.storage.from_(self.bucket_name).download(file_path)
            
            if result:
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                    temp_file.write(result)
                    temp_file_path = temp_file.name
                
                try:
                    df = pd.read_parquet(temp_file_path)
                    return df
                finally:
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
            
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
            # Monta caminhos considerando o trimestre; mantém compatibilidade tentando caminhos antigos
            candidate_paths: List[str] = []
            if quarter:
                candidate_paths.extend([
                    f"{quarter}/{snapshot_id}/commits.parquet",
                    f"{quarter}/{snapshot_id}/pull_requests.parquet",
                    f"{quarter}/{snapshot_id}/metadata.json",
                ])
            # Caminhos sem trimestre (legado)
            candidate_paths.extend([
                f"{snapshot_id}/commits.parquet",
                f"{snapshot_id}/pull_requests.parquet",
                f"{snapshot_id}/metadata.json",
            ])

            # Remove em lotes, ignorando erros de arquivos inexistentes
            try:
                self.client.storage.from_(self.bucket_name).remove(candidate_paths)
            except Exception:
                # Tenta individualmente para maior tolerância
                for file_path in candidate_paths:
                    try:
                        self.client.storage.from_(self.bucket_name).remove([file_path])
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
            return {
                'repository_name': repo_name,
                'total_snapshots': 0,
                'latest_snapshot': None,
                'total_commits': 0,
                'total_pull_requests': 0,
                'snapshots': []
            }
