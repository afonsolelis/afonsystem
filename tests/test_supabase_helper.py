import pytest
from unittest.mock import Mock, patch, MagicMock
import os
import pandas as pd
from helpers.supabase_helper import SupabaseHelper


class TestSupabaseHelper:
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_init_success(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        
        helper = SupabaseHelper()
        
        assert helper.url == 'https://test.supabase.co'
        assert helper.key == 'test_key'
        assert helper.bucket_name == 'afonsystem'
        assert helper.client == mock_client
        assert helper.snapshot_manager == mock_snapshot_instance
        mock_create_client.assert_called_once_with('https://test.supabase.co', 'test_key')
    
    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_env_vars(self):
        with pytest.raises(ValueError, match="SUPABASE_URL and SUPABASE_ANON_KEY must be set"):
            SupabaseHelper()
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_create_parquet_snapshot(self, mock_snapshot_manager, mock_create_client):
        mock_create_client.return_value = Mock()
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        mock_snapshot_instance.create_repository_snapshot.return_value = 'snapshot_123'
        
        helper = SupabaseHelper()
        
        commits_data = [{'sha': 'abc123', 'message': 'test commit'}]
        prs_data = [{'number': 1, 'title': 'test pr'}]
        
        result = helper.create_parquet_snapshot('owner/repo', commits_data, prs_data, '2025-1B')
        
        assert result == 'snapshot_123'
        mock_snapshot_instance.create_repository_snapshot.assert_called_once_with(
            'owner/repo', commits_data, prs_data, '2025-1B'
        )
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_list_parquet_snapshots(self, mock_snapshot_manager, mock_create_client):
        mock_create_client.return_value = Mock()
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        
        expected_snapshots = [
            {'snapshot_id': 'snap1', 'timestamp': '2023-01-01'},
            {'snapshot_id': 'snap2', 'timestamp': '2023-01-02'}
        ]
        mock_snapshot_instance.list_repository_snapshots.return_value = expected_snapshots
        
        helper = SupabaseHelper()
        result = helper.list_parquet_snapshots('owner/repo', '2025-1B')
        
        assert result == expected_snapshots
        mock_snapshot_instance.list_repository_snapshots.assert_called_once_with('owner/repo', '2025-1B')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_load_snapshot_data(self, mock_snapshot_manager, mock_create_client):
        mock_create_client.return_value = Mock()
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        
        expected_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_snapshot_instance.load_snapshot_data.return_value = expected_df
        
        helper = SupabaseHelper()
        result = helper.load_snapshot_data('snapshot_123', 'commits', '2025-1B')
        
        assert result.equals(expected_df)
        mock_snapshot_instance.load_snapshot_data.assert_called_once_with('snapshot_123', 'commits', '2025-1B')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_delete_parquet_snapshot(self, mock_snapshot_manager, mock_create_client):
        mock_create_client.return_value = Mock()
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        mock_snapshot_instance.delete_snapshot.return_value = True
        
        helper = SupabaseHelper()
        result = helper.delete_parquet_snapshot('snapshot_123')
        
        assert result is True
        mock_snapshot_instance.delete_snapshot.assert_called_once_with('snapshot_123')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_get_snapshot_summary(self, mock_snapshot_manager, mock_create_client):
        mock_create_client.return_value = Mock()
        mock_snapshot_instance = Mock()
        mock_snapshot_manager.return_value = mock_snapshot_instance
        
        expected_summary = {
            'repository_name': 'owner/repo',
            'total_snapshots': 2,
            'total_commits': 15
        }
        mock_snapshot_instance.export_snapshot_summary.return_value = expected_summary
        
        helper = SupabaseHelper()
        result = helper.get_snapshot_summary('owner/repo')
        
        assert result == expected_summary
        mock_snapshot_instance.export_snapshot_summary.assert_called_once_with('owner/repo')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_list_snapshots_no_filter(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        
        mock_list_result = [
            {'name': 'repo1_snapshot.zip', 'size': 1024},
            {'name': 'repo2_snapshot.zip', 'size': 2048},
            {'name': 'other_file.txt', 'size': 512}
        ]
        mock_client.storage.from_().list.return_value = mock_list_result
        
        helper = SupabaseHelper()
        result = helper.list_snapshots()
        
        assert len(result) == 2
        assert all(file['name'].endswith('_snapshot.zip') for file in result)
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_list_snapshots_with_filter(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        
        mock_list_result = [
            {'name': 'repo1_snapshot.zip', 'size': 1024},
            {'name': 'repo2_snapshot.zip', 'size': 2048},
            {'name': 'other_snapshot.zip', 'size': 512}
        ]
        mock_client.storage.from_().list.return_value = mock_list_result
        
        helper = SupabaseHelper()
        result = helper.list_snapshots('repo1')
        
        assert len(result) == 1
        assert result[0]['name'] == 'repo1_snapshot.zip'
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_list_snapshots_error(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        mock_client.storage.from_().list.side_effect = Exception('Storage error')
        
        helper = SupabaseHelper()
        
        with pytest.raises(Exception, match='Error listing snapshots'):
            helper.list_snapshots()
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_get_snapshot_url(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        mock_client.storage.from_().get_public_url.return_value = 'https://example.com/snapshot.zip'
        
        helper = SupabaseHelper()
        result = helper.get_snapshot_url('test_snapshot.zip')
        
        assert result == 'https://example.com/snapshot.zip'
        mock_client.storage.from_().get_public_url.assert_called_once_with('test_snapshot.zip')
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_delete_snapshot_success(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        
        mock_remove_result = Mock()
        mock_remove_result.error = None
        mock_client.storage.from_().remove.return_value = mock_remove_result
        
        helper = SupabaseHelper()
        result = helper.delete_snapshot('test_snapshot.zip')
        
        assert result is True
        mock_client.storage.from_().remove.assert_called_once_with(['test_snapshot.zip'])
    
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_delete_snapshot_error(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        
        mock_remove_result = Mock()
        mock_remove_result.error = 'Delete error'
        mock_client.storage.from_().remove.return_value = mock_remove_result
        
        helper = SupabaseHelper()
        result = helper.delete_snapshot('test_snapshot.zip')
        
        assert result is False
        
    @patch.dict(os.environ, {'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'})
    @patch('helpers.supabase_helper.create_client')
    @patch('helpers.supabase_helper.SnapshotManager')
    def test_delete_snapshot_exception(self, mock_snapshot_manager, mock_create_client):
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        mock_snapshot_manager.return_value = Mock()
        mock_client.storage.from_().remove.side_effect = Exception('Storage error')
        
        helper = SupabaseHelper()
        
        with pytest.raises(Exception, match='Error deleting snapshot'):
            helper.delete_snapshot('test_snapshot.zip')