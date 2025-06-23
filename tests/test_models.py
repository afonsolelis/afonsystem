import pytest
from datetime import datetime
from pydantic import ValidationError
from models.commit import Commit
from models.pull_request import PullRequest
from models.snapshot import SnapshotMetadata, SnapshotSummary


class TestCommit:
    
    def test_commit_valid_creation(self):
        commit_data = {
            'sha': 'abc123def456',
            'message': 'feat: add new feature',
            'author': 'John Doe',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/commit/abc123def456'
        }
        
        commit = Commit(**commit_data)
        
        assert commit.sha == 'abc123def456'
        assert commit.message == 'feat: add new feature'
        assert commit.author == 'John Doe'
        assert commit.date == datetime(2023, 1, 1, 12, 0, 0)
        assert commit.url == 'https://github.com/owner/repo/commit/abc123def456'
    
    def test_commit_string_whitespace_strip(self):
        commit_data = {
            'sha': '  abc123def456  ',
            'message': '  feat: add new feature  ',
            'author': '  John Doe  ',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': '  https://github.com/owner/repo/commit/abc123def456  '
        }
        
        commit = Commit(**commit_data)
        
        assert commit.sha == 'abc123def456'
        assert commit.message == 'feat: add new feature'
        assert commit.author == 'John Doe'
        assert commit.url == 'https://github.com/owner/repo/commit/abc123def456'
    
    def test_commit_empty_sha_validation_error(self):
        commit_data = {
            'sha': '',
            'message': 'feat: add new feature',
            'author': 'John Doe',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/commit/abc123def456'
        }
        
        with pytest.raises(ValidationError):
            Commit(**commit_data)
    
    def test_commit_empty_message_validation_error(self):
        commit_data = {
            'sha': 'abc123def456',
            'message': '',
            'author': 'John Doe',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/commit/abc123def456'
        }
        
        with pytest.raises(ValidationError):
            Commit(**commit_data)
    
    def test_commit_empty_author_validation_error(self):
        commit_data = {
            'sha': 'abc123def456',
            'message': 'feat: add new feature',
            'author': '',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/commit/abc123def456'
        }
        
        with pytest.raises(ValidationError):
            Commit(**commit_data)
    
    def test_commit_empty_url_validation_error(self):
        commit_data = {
            'sha': 'abc123def456',
            'message': 'feat: add new feature',
            'author': 'John Doe',
            'date': datetime(2023, 1, 1, 12, 0, 0),
            'url': ''
        }
        
        with pytest.raises(ValidationError):
            Commit(**commit_data)
    
    def test_commit_missing_required_field(self):
        commit_data = {
            'sha': 'abc123def456',
            'message': 'feat: add new feature',
            'author': 'John Doe',
            'date': datetime(2023, 1, 1, 12, 0, 0)
        }
        
        with pytest.raises(ValidationError):
            Commit(**commit_data)


class TestPullRequest:
    
    def test_pull_request_valid_creation(self):
        pr_data = {
            'number': 123,
            'title': 'Add new feature',
            'author': 'jane_doe', 
            'state': 'open',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/pull/123'
        }
        
        pr = PullRequest(**pr_data)
        
        assert pr.number == 123
        assert pr.title == 'Add new feature'
        assert pr.author == 'jane_doe'
        assert pr.state == 'open'
        assert pr.created_at == datetime(2023, 1, 1, 12, 0, 0)
        assert pr.url == 'https://github.com/owner/repo/pull/123'
    
    def test_pull_request_string_whitespace_strip(self):
        pr_data = {
            'number': 123,
            'title': '  Add new feature  ',
            'author': '  jane_doe  ',
            'state': '  open  ',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': '  https://github.com/owner/repo/pull/123  '
        }
        
        pr = PullRequest(**pr_data)
        
        assert pr.title == 'Add new feature'
        assert pr.author == 'jane_doe'
        assert pr.state == 'open'
        assert pr.url == 'https://github.com/owner/repo/pull/123'
    
    def test_pull_request_negative_number_validation_error(self):
        pr_data = {
            'number': -1,
            'title': 'Add new feature',
            'author': 'jane_doe',
            'state': 'open',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/pull/123'
        }
        
        with pytest.raises(ValidationError):
            PullRequest(**pr_data)
    
    def test_pull_request_empty_title_validation_error(self):
        pr_data = {
            'number': 123,
            'title': '',
            'author': 'jane_doe',
            'state': 'open',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/pull/123'
        }
        
        with pytest.raises(ValidationError):
            PullRequest(**pr_data)
    
    def test_pull_request_empty_author_validation_error(self):
        pr_data = {
            'number': 123,
            'title': 'Add new feature',
            'author': '',
            'state': 'open',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/pull/123'
        }
        
        with pytest.raises(ValidationError):
            PullRequest(**pr_data)
    
    def test_pull_request_empty_state_validation_error(self):
        pr_data = {
            'number': 123,
            'title': 'Add new feature',
            'author': 'jane_doe',
            'state': '',
            'created_at': datetime(2023, 1, 1, 12, 0, 0),
            'url': 'https://github.com/owner/repo/pull/123'
        }
        
        with pytest.raises(ValidationError):
            PullRequest(**pr_data)
    
    def test_pull_request_valid_states(self):
        valid_states = ['open', 'closed', 'merged']
        
        for state in valid_states:
            pr_data = {
                'number': 123,
                'title': 'Add new feature',
                'author': 'jane_doe',
                'state': state,
                'created_at': datetime(2023, 1, 1, 12, 0, 0),
                'url': 'https://github.com/owner/repo/pull/123'
            }
            
            pr = PullRequest(**pr_data)
            assert pr.state == state


class TestSnapshotMetadata:
    
    def test_snapshot_metadata_valid_creation(self):
        snapshot_data = {
            'timestamp': '2023-01-01_12-00-00',
            'repository_name': 'owner/repo',
            'commits_count': 50,
            'pull_requests_count': 10,
            'snapshot_id': 'snapshot_123',
            'created_at': '2023-01-01T12:00:00Z'
        }
        
        snapshot = SnapshotMetadata(**snapshot_data)
        
        assert snapshot.timestamp == '2023-01-01_12-00-00'
        assert snapshot.repository_name == 'owner/repo'
        assert snapshot.commits_count == 50
        assert snapshot.pull_requests_count == 10
        assert snapshot.snapshot_id == 'snapshot_123'
        assert snapshot.created_at == '2023-01-01T12:00:00Z'
    
    def test_snapshot_metadata_string_whitespace_strip(self):
        snapshot_data = {
            'timestamp': '  2023-01-01_12-00-00  ',
            'repository_name': '  owner/repo  ',
            'commits_count': 50,
            'pull_requests_count': 10,
            'snapshot_id': '  snapshot_123  ',
            'created_at': '  2023-01-01T12:00:00Z  '
        }
        
        snapshot = SnapshotMetadata(**snapshot_data)
        
        assert snapshot.timestamp == '2023-01-01_12-00-00'
        assert snapshot.repository_name == 'owner/repo'
        assert snapshot.snapshot_id == 'snapshot_123'
        assert snapshot.created_at == '2023-01-01T12:00:00Z'
    
    def test_snapshot_metadata_negative_counts_validation_error(self):
        snapshot_data = {
            'timestamp': '2023-01-01_12-00-00',
            'repository_name': 'owner/repo',
            'commits_count': -1,
            'pull_requests_count': 10,
            'snapshot_id': 'snapshot_123',
            'created_at': '2023-01-01T12:00:00Z'
        }
        
        with pytest.raises(ValidationError):
            SnapshotMetadata(**snapshot_data)
    
    def test_snapshot_metadata_empty_timestamp_validation_error(self):
        snapshot_data = {
            'timestamp': '',
            'repository_name': 'owner/repo',
            'commits_count': 50,
            'pull_requests_count': 10,
            'snapshot_id': 'snapshot_123',
            'created_at': '2023-01-01T12:00:00Z'
        }
        
        with pytest.raises(ValidationError):
            SnapshotMetadata(**snapshot_data)
    
    def test_snapshot_metadata_empty_repository_name_validation_error(self):
        snapshot_data = {
            'timestamp': '2023-01-01_12-00-00',
            'repository_name': '',
            'commits_count': 50,
            'pull_requests_count': 10,
            'snapshot_id': 'snapshot_123',
            'created_at': '2023-01-01T12:00:00Z'
        }
        
        with pytest.raises(ValidationError):
            SnapshotMetadata(**snapshot_data)
    
    def test_snapshot_metadata_zero_counts_valid(self):
        snapshot_data = {
            'timestamp': '2023-01-01_12-00-00',
            'repository_name': 'owner/repo',
            'commits_count': 0,
            'pull_requests_count': 0,
            'snapshot_id': 'snapshot_123',
            'created_at': '2023-01-01T12:00:00Z'
        }
        
        snapshot = SnapshotMetadata(**snapshot_data)
        
        assert snapshot.commits_count == 0
        assert snapshot.pull_requests_count == 0


class TestSnapshotSummary:
    
    def test_snapshot_summary_valid_creation(self):
        latest_snapshot = SnapshotMetadata(
            timestamp='2023-01-01_12-00-00',
            repository_name='owner/repo',
            commits_count=25,
            pull_requests_count=5,
            snapshot_id='snapshot_latest',
            created_at='2023-01-01T12:00:00Z'
        )
        
        summary_data = {
            'repository_name': 'owner/repo',
            'total_snapshots': 3,
            'latest_snapshot': latest_snapshot,
            'total_commits': 100,
            'total_pull_requests': 15
        }
        
        summary = SnapshotSummary(**summary_data)
        
        assert summary.repository_name == 'owner/repo'
        assert summary.total_snapshots == 3
        assert summary.latest_snapshot == latest_snapshot
        assert summary.total_commits == 100
        assert summary.total_pull_requests == 15
    
    def test_snapshot_summary_no_latest_snapshot(self):
        summary_data = {
            'repository_name': 'owner/repo',
            'total_snapshots': 0,
            'latest_snapshot': None,
            'total_commits': 0,
            'total_pull_requests': 0
        }
        
        summary = SnapshotSummary(**summary_data)
        
        assert summary.repository_name == 'owner/repo'
        assert summary.total_snapshots == 0
        assert summary.latest_snapshot is None
        assert summary.total_commits == 0
        assert summary.total_pull_requests == 0
    
    def test_snapshot_summary_negative_counts_validation_error(self):
        summary_data = {
            'repository_name': 'owner/repo',
            'total_snapshots': -1,
            'latest_snapshot': None,
            'total_commits': 0,
            'total_pull_requests': 0
        }
        
        with pytest.raises(ValidationError):
            SnapshotSummary(**summary_data)
    
    def test_snapshot_summary_empty_repository_name_validation_error(self):
        summary_data = {
            'repository_name': '',
            'total_snapshots': 0,
            'latest_snapshot': None,
            'total_commits': 0,
            'total_pull_requests': 0
        }
        
        with pytest.raises(ValidationError):
            SnapshotSummary(**summary_data)