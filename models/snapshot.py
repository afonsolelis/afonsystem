from pydantic import BaseModel, Field
from typing import Optional


class SnapshotMetadata(BaseModel):
    """Pydantic model for snapshot metadata"""
    timestamp: str = Field(..., min_length=1, description="Snapshot timestamp")
    repository_name: str = Field(..., min_length=1, description="Repository name")
    commits_count: int = Field(..., ge=0, description="Number of commits in snapshot")
    pull_requests_count: int = Field(..., ge=0, description="Number of pull requests in snapshot")
    snapshot_id: str = Field(..., min_length=1, description="Unique snapshot identifier")
    created_at: str = Field(..., min_length=1, description="Creation timestamp in ISO format")
    
    class Config:
        validate_assignment = True
        str_strip_whitespace = True


class SnapshotSummary(BaseModel):
    """Summary of all snapshots for a repository"""
    repository_name: str = Field(..., min_length=1, description="Repository name")
    total_snapshots: int = Field(..., ge=0, description="Total number of snapshots")
    latest_snapshot: Optional[SnapshotMetadata] = Field(None, description="Most recent snapshot metadata")
    total_commits: int = Field(..., ge=0, description="Total commits across all snapshots")
    total_pull_requests: int = Field(..., ge=0, description="Total pull requests across all snapshots")
    
    class Config:
        validate_assignment = True
        str_strip_whitespace = True