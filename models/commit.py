from pydantic import BaseModel, Field
from datetime import datetime


class Commit(BaseModel):
    sha: str = Field(..., min_length=1, description="Commit SHA hash")
    message: str = Field(..., min_length=1, description="Commit message")
    author: str = Field(..., min_length=1, description="Commit author")
    date: datetime = Field(..., description="Commit date")
    url: str = Field(..., min_length=1, description="Commit URL")
    
    class Config:
        validate_assignment = True
        str_strip_whitespace = True