import pytest
from unittest.mock import Mock, patch, MagicMock
import os
from datetime import datetime
from helpers.data_collector import GitHubDataCollector


class TestGitHubDataCollector:
    
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token', 'REPO_NAMES': 'owner/repo1,owner/repo2'})
    def test_init(self):
        collector = GitHubDataCollector()
        assert collector.github_token == 'test_token'
        assert collector.repo_names == ['owner/repo1', 'owner/repo2']
        assert collector._github is None
        assert collector.snapshot_manager is not None
    
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token', 'REPO_NAMES': ''})
    def test_init_empty_repo_names(self):
        collector = GitHubDataCollector()
        assert collector.repo_names == []
    
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token', 'REPO_NAMES': 'owner/repo1, owner/repo2 , owner/repo3'})
    def test_get_repo_names_with_spaces(self):
        collector = GitHubDataCollector()
        assert collector.repo_names == ['owner/repo1', 'owner/repo2', 'owner/repo3']
    
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token', 'SUPABASE_URL': 'https://test.supabase.co', 'SUPABASE_ANON_KEY': 'test_key'}, clear=True)
    @patch('helpers.snapshot_manager.create_client')
    def test_get_repo_names_missing_env(self, mock_create_client):
        mock_create_client.return_value = Mock()
        collector = GitHubDataCollector()
        assert collector.repo_names == []
    
    def test_repo_name_to_snake_case(self):
        collector = GitHubDataCollector()
        assert collector._repo_name_to_snake_case('owner/repo-name') == 'owner_repo_name'
        assert collector._repo_name_to_snake_case('owner/repo.name') == 'owner_reponame'
        assert collector._repo_name_to_snake_case('OWNER/REPO-NAME') == 'owner_repo_name'
        assert collector._repo_name_to_snake_case('owner/repo@name') == 'owner_reponame'
    
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token', 'REPO_NAMES': 'owner/repo1,owner/repo2'})
    def test_get_available_repos(self):
        collector = GitHubDataCollector()
        assert collector.get_available_repos() == ['owner/repo1', 'owner/repo2']
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_github_property_lazy_init(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        
        collector = GitHubDataCollector()
        assert collector._github is None
        
        github_client = collector.github
        assert github_client == mock_github_instance
        assert collector._github == mock_github_instance
        mock_github_class.assert_called_once_with('test_token')
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_collect_and_create_snapshot_success(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        
        mock_repo = Mock()
        mock_github_instance.get_repo.return_value = mock_repo
        
        mock_commit = Mock()
        mock_commit.sha = 'abc123'
        mock_commit.commit.message = 'test commit'
        mock_commit.commit.author.name = 'Test Author'
        mock_commit.commit.author.date = datetime(2023, 1, 1, 12, 0, 0)
        mock_commit.html_url = 'https://github.com/owner/repo/commit/abc123'
        mock_repo.get_commits.return_value = [mock_commit]
        
        mock_pr = Mock()
        mock_pr.number = 1
        mock_pr.title = 'Test PR'
        mock_pr.user.login = 'testuser'
        mock_pr.state = 'open'
        mock_pr.created_at = datetime(2023, 1, 1, 12, 0, 0)
        mock_pr.html_url = 'https://github.com/owner/repo/pull/1'
        mock_repo.get_pulls.return_value = [mock_pr]
        
        collector = GitHubDataCollector()
        
        with patch.object(collector.snapshot_manager, 'create_repository_snapshot') as mock_create_snapshot:
            mock_create_snapshot.return_value = 'snapshot_123'
            
            progress_messages = []
            def progress_callback(message):
                progress_messages.append(message)
            
            result = collector.collect_and_create_snapshot('owner/repo', progress_callback, '2025-1B')
            
            assert result == 'snapshot_123'
            assert len(progress_messages) > 0
            assert '🔍 Starting data collection for owner/repo...' in progress_messages
            assert '✅ Snapshot created successfully: snapshot_123' in progress_messages
            
            mock_create_snapshot.assert_called_once()
            call_args = mock_create_snapshot.call_args
            assert call_args[0][0] == 'owner/repo'
            assert len(call_args[0][1]) == 1
            assert len(call_args[0][2]) == 1
            assert call_args[0][3] == '2025-1B'
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_collect_and_create_snapshot_github_error(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        mock_github_instance.get_repo.side_effect = Exception('GitHub API Error')
        
        collector = GitHubDataCollector()
        
        progress_messages = []
        def progress_callback(message):
            progress_messages.append(message)
        
        result = collector.collect_and_create_snapshot('owner/repo', progress_callback)
        
        assert result is None
        assert any('❌ Error collecting data for owner/repo' in msg for msg in progress_messages)
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_collect_and_create_snapshot_commit_error(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        
        mock_repo = Mock()
        mock_github_instance.get_repo.return_value = mock_repo
        
        mock_commit_good = Mock()
        mock_commit_good.sha = 'abc123'
        mock_commit_good.commit.message = 'test commit'
        mock_commit_good.commit.author.name = 'Test Author'
        mock_commit_good.commit.author.date = datetime(2023, 1, 1, 12, 0, 0)
        mock_commit_good.html_url = 'https://github.com/owner/repo/commit/abc123'
        
        mock_commit_bad = Mock()
        mock_commit_bad.sha = 'def456'
        mock_commit_bad.commit.message = 'test commit 2'
        mock_commit_bad.commit.author = None
        mock_commit_bad.html_url = 'https://github.com/owner/repo/commit/def456'
        
        mock_repo.get_commits.return_value = [mock_commit_good, mock_commit_bad]
        mock_repo.get_pulls.return_value = []
        
        collector = GitHubDataCollector()
        
        with patch.object(collector.snapshot_manager, 'create_repository_snapshot') as mock_create_snapshot:
            mock_create_snapshot.return_value = 'snapshot_123'
            
            result = collector.collect_and_create_snapshot('owner/repo')
            
            assert result == 'snapshot_123'
            call_args = mock_create_snapshot.call_args
            assert len(call_args[0][1]) == 2
            
            commits_data = call_args[0][1]
            assert commits_data[0]['author'] == 'Test Author'
            assert commits_data[1]['author'] == 'Unknown'
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_collect_and_create_snapshot_no_progress_callback(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        
        mock_repo = Mock()
        mock_github_instance.get_repo.return_value = mock_repo
        mock_repo.get_commits.return_value = []
        mock_repo.get_pulls.return_value = []
        
        collector = GitHubDataCollector()
        
        with patch.object(collector.snapshot_manager, 'create_repository_snapshot') as mock_create_snapshot:
            mock_create_snapshot.return_value = 'snapshot_123'
            
            result = collector.collect_and_create_snapshot('owner/repo')
            assert result == 'snapshot_123'
    
    @patch('helpers.data_collector.Github')
    @patch.dict(os.environ, {'GITHUB_TOKEN': 'test_token'})
    def test_collect_and_create_snapshot_snapshot_creation_failure(self, mock_github_class):
        mock_github_instance = Mock()
        mock_github_class.return_value = mock_github_instance
        
        mock_repo = Mock()
        mock_github_instance.get_repo.return_value = mock_repo
        mock_repo.get_commits.return_value = []
        mock_repo.get_pulls.return_value = []
        
        collector = GitHubDataCollector()
        
        with patch.object(collector.snapshot_manager, 'create_repository_snapshot') as mock_create_snapshot:
            mock_create_snapshot.return_value = None
            
            progress_messages = []
            def progress_callback(message):
                progress_messages.append(message)
            
            result = collector.collect_and_create_snapshot('owner/repo', progress_callback)
            
            assert result is None
            assert any('❌ Failed to create snapshot' in msg for msg in progress_messages)