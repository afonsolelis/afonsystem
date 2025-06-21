from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class PullRequest(BaseModel):
    number: int = Field(..., gt=0, description="Pull request number")
    title: str = Field(..., min_length=1, description="Pull request title")
    author: str = Field(..., min_length=1, description="Pull request author")
    state: str = Field(..., min_length=1, description="Pull request state")
    created_at: datetime = Field(..., description="Pull request creation date")
    url: str = Field(..., min_length=1, description="Pull request URL")
    
    class Config:
        validate_assignment = True
        str_strip_whitespace = True