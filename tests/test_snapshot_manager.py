import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
import os
import json
import tempfile
import pandas as pd
from datetime import datetime
from helpers.snapshot_manager import SnapshotManager


class TestSnapshotManager:
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_init_success(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        manager = SnapshotManager()
        
        assert manager.url == 'https://test.supabase.co'
        assert manager.key == 'test_key'
        assert manager.bucket_name == 'afonsystem'
        assert manager.client == mock_client
        mock_create_client.assert_called_once_with('https://test.supabase.co', 'test_key')
    
    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_env_vars(self):
        with pytest.raises(ValueError, match="SUPABASE_URL and SUPABASE_ANON_KEY must be set"):
            SnapshotManager()
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co'}, clear=True)
    def test_init_missing_key(self):
        with pytest.raises(ValueError, match="SUPABASE_URL and SUPABASE_ANON_KEY must be set"):
            SnapshotManager()
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_get_content_type(self, mock_create_client):
        mock_create_client.return_value = Mock()
        manager = SnapshotManager()
        
        assert manager._get_content_type('test.parquet') == 'application/octet-stream'
        assert manager._get_content_type('test.json') == 'application/json'
        assert manager._get_content_type('test.txt') == 'application/octet-stream'
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    @patch('tempfile.TemporaryDirectory')
    @patch('pandas.DataFrame.to_parquet')
    @patch('builtins.open', new_callable=mock_open)
    def test_create_repository_snapshot_success(self, mock_file_open, mock_to_parquet, mock_temp_dir, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        mock_temp_dir.return_value.__enter__ = Mock(return_value='/tmp/test')
        mock_temp_dir.return_value.__exit__ = Mock(return_value=None)
        
        mock_upload_result = Mock()
        mock_upload_result.error = None
        mock_client.storage.from_().upload.return_value = mock_upload_result
        
        manager = SnapshotManager()
        
        commits_data = [{'sha': 'abc123', 'message': 'test commit'}]
        prs_data = [{'number': 1, 'title': 'test pr'}]
        
        with patch.object(manager, '_upload_file') as mock_upload:
            result = manager.create_repository_snapshot('owner/repo', commits_data, prs_data, '2025-1B')
            
            assert result.startswith('snapshot_owner_repo_')
            assert mock_upload.call_count == 3
            
            upload_calls = [call[0] for call in mock_upload.call_args_list]
            assert any('commits.parquet' in call[1] for call in upload_calls)
            assert any('pull_requests.parquet' in call[1] for call in upload_calls)
            assert any('metadata.json' in call[1] for call in upload_calls)
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    @patch('tempfile.TemporaryDirectory')
    @patch('pandas.DataFrame.to_parquet')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_create_repository_snapshot_no_prs(self, mock_makedirs, mock_file_open, mock_to_parquet, mock_temp_dir, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        mock_temp_dir.return_value.__enter__ = Mock(return_value='/tmp/test')
        mock_temp_dir.return_value.__exit__ = Mock(return_value=None)
        
        manager = SnapshotManager()
        
        commits_data = [{'sha': 'abc123', 'message': 'test commit'}]
        
        with patch.object(manager, '_upload_file') as mock_upload:
            result = manager.create_repository_snapshot('owner/repo', commits_data, None, '2025-1B')
            
            assert result.startswith('snapshot_owner_repo_')
            assert mock_upload.call_count == 2
            
            upload_calls = [call[0] for call in mock_upload.call_args_list]
            assert any('commits.parquet' in call[1] for call in upload_calls)
            assert any('metadata.json' in call[1] for call in upload_calls)
            assert not any('pull_requests.parquet' in call[1] for call in upload_calls)
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_create_repository_snapshot_upload_error(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        manager = SnapshotManager()
        
        with patch.object(manager, '_upload_file', side_effect=Exception('Upload failed')):
            with pytest.raises(Exception, match='Error creating snapshot for owner/repo'):
                manager.create_repository_snapshot('owner/repo', [], None, '2025-1B')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test content')
    def test_upload_file_success(self, mock_file_open, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        mock_upload_result = Mock()
        mock_upload_result.error = None
        mock_client.storage.from_().upload.return_value = mock_upload_result
        
        manager = SnapshotManager()
        manager._upload_file('/local/path/file.txt', 'remote/path/file.txt')
        
        mock_client.storage.from_.assert_called_with('afonsystem')
        mock_client.storage.from_().upload.assert_called_once()
        
        call_args = mock_client.storage.from_().upload.call_args
        assert call_args[1]['path'] == 'remote/path/file.txt'
        assert call_args[1]['file'] == b'test content'
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test content')
    def test_upload_file_error(self, mock_file_open, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        mock_upload_result = Mock()
        mock_upload_result.error = 'Upload failed'
        mock_client.storage.from_().upload.return_value = mock_upload_result
        
        manager = SnapshotManager()
        
        with pytest.raises(Exception, match='Failed to upload remote/path/file.txt'):
            manager._upload_file('/local/path/file.txt', 'remote/path/file.txt')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_list_repository_snapshots_success(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        mock_list_result = [
            {'name': 'snapshot_owner_repo_2023-01-01_12-00-00', 'created_at': '2023-01-01T12:00:00Z'}
        ]
        mock_client.storage.from_().list.return_value = mock_list_result
        
        manager = SnapshotManager()
        
        with patch.object(manager, 'get_snapshot_metadata') as mock_get_metadata:
            mock_get_metadata.side_effect = [
                {'snapshot_id': 'snapshot_owner_repo_2023-01-01_12-00-00', 'timestamp': '2023-01-01_12-00-00'}
            ]
            
            result = manager.list_repository_snapshots('owner/repo', '2025-1B')
            
            assert len(result) == 1
            assert result[0]['timestamp'] == '2023-01-01_12-00-00'
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_list_repository_snapshots_empty(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_client.storage.from_().list.return_value = []
        
        manager = SnapshotManager()
        result = manager.list_repository_snapshots('owner/repo', '2025-1B')
        
        assert result == []
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_get_snapshot_metadata_success(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        metadata = {'snapshot_id': 'test_snapshot', 'timestamp': '2023-01-01_12-00-00'}
        mock_client.storage.from_().download.return_value = json.dumps(metadata).encode('utf-8')
        
        manager = SnapshotManager()
        result = manager.get_snapshot_metadata('test_snapshot', '2025-1B')
        
        assert result == metadata
        mock_client.storage.from_().download.assert_called_once_with('2025-1B/test_snapshot/metadata.json')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_get_snapshot_metadata_not_found(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_client.storage.from_().download.return_value = None
        
        manager = SnapshotManager()
        result = manager.get_snapshot_metadata('test_snapshot', '2025-1B')
        
        assert result is None
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    @patch('tempfile.NamedTemporaryFile')
    @patch('pandas.read_parquet')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_load_snapshot_data_success(self, mock_unlink, mock_exists, mock_read_parquet, mock_temp_file, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        test_data = b'parquet_data'
        mock_client.storage.from_().download.return_value = test_data
        
        mock_temp_file_instance = Mock()
        mock_temp_file_instance.name = '/tmp/test.parquet'
        mock_temp_file.return_value.__enter__ = Mock(return_value=mock_temp_file_instance)
        mock_temp_file.return_value.__exit__ = Mock(return_value=None)
        
        mock_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_read_parquet.return_value = mock_df
        mock_exists.return_value = True
        
        manager = SnapshotManager()
        result = manager.load_snapshot_data('test_snapshot', 'commits', '2025-1B')
        
        assert result.equals(mock_df)
        mock_client.storage.from_().download.assert_called_once_with('2025-1B/test_snapshot/commits.parquet')
        mock_temp_file_instance.write.assert_called_once_with(test_data)
        mock_read_parquet.assert_called_once_with('/tmp/test.parquet')
        mock_unlink.assert_called_once_with('/tmp/test.parquet')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_load_snapshot_data_not_found(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_client.storage.from_().download.return_value = None
        
        manager = SnapshotManager()
        result = manager.load_snapshot_data('test_snapshot', 'commits', '2025-1B')
        
        assert result is None
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_delete_snapshot_success(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        manager = SnapshotManager()
        result = manager.delete_snapshot('test_snapshot')
        
        assert result is True
        assert mock_client.storage.from_().remove.call_count == 3
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_get_snapshot_url(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_client.storage.from_().get_public_url.return_value = 'https://example.com/file.parquet'
        
        manager = SnapshotManager()
        result = manager.get_snapshot_url('test_snapshot', 'commits')
        
        assert result == 'https://example.com/file.parquet'
        mock_client.storage.from_().get_public_url.assert_called_once_with('test_snapshot/commits.parquet')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_export_snapshot_summary_with_snapshots(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        manager = SnapshotManager()
        
        mock_snapshots = [
            {'commits_count': 10, 'pull_requests_count': 2, 'timestamp': '2023-01-01_12-00-00'},
            {'commits_count': 5, 'pull_requests_count': 1, 'timestamp': '2023-01-01_11-00-00'}
        ]
        
        with patch.object(manager, 'list_repository_snapshots', return_value=mock_snapshots):
            result = manager.export_snapshot_summary('owner/repo')
            
            assert result['repository_name'] == 'owner/repo'
            assert result['total_snapshots'] == 2
            assert result['total_commits'] == 15
            assert result['total_pull_requests'] == 3
            assert result['latest_snapshot'] == mock_snapshots[0]
            assert result['snapshots'] == mock_snapshots
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.snapshot_manager.create_client')
    def test_export_snapshot_summary_no_snapshots(self, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        manager = SnapshotManager()
        
        with patch.object(manager, 'list_repository_snapshots', return_value=[]):
            result = manager.export_snapshot_summary('owner/repo')
            
            assert result['repository_name'] == 'owner/repo'
            assert result['total_snapshots'] == 0
            assert result['latest_snapshot'] is None
            assert result['total_commits'] == 0
            assert result['total_pull_requests'] == 0
            assert result['snapshots'] == []