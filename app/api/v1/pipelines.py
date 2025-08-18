from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.models.pipeline import Pipeline, PipelineRun
from app.schemas.pipeline import PipelineCreate, PipelineRunCreate
from app.services.orchestrator import Orchestrator
from typing import List

router = APIRouter()
orchestrator = Orchestrator()

@router.post("/pipelines/")
def create_pipeline(pipeline: PipelineCreate, db: Session = Depends(get_db)):
    """Create a new pipeline"""
    print("Creating  sample pipeline pipeline")
    created_pipeline = orchestrator.create_sample_pipeline(db)
    return created_pipeline

@router.get("/pipelines/")
def get_pipelines(db: Session = Depends(get_db)):
    """Get all pipelines"""
    return db.query(Pipeline).all()

@router.post("/pipelines/{pipeline_id}/execute")
def execute_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    """Execute a pipeline"""
    try:
        # Create pipeline run
        pipeline_run = orchestrator.create_pipeline_run(db, pipeline_id)
        
        # Start DAG resolution and task dispatch
        orchestrator.resolve_dag_and_dispatch(db, pipeline_run.id)
        
        return pipeline_run
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipelines/{pipeline_id}/runs")
def get_pipeline_runs(pipeline_id: int, db: Session = Depends(get_db)):
    """Get all runs for a pipeline"""
    return db.query(PipelineRun).filter(PipelineRun.pipeline_id == pipeline_id).all()