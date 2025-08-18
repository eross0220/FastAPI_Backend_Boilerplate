from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

class PipelineCreate(BaseModel):
    pass
class Pipeline(PipelineCreate):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class PipelineRunCreate(BaseModel):
    pass

class PipelineRun(PipelineRunCreate):
    id: int
    pipeline_id: int
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True