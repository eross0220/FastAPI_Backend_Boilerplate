from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.models.pipeline import Pipeline, PipelineRun
from app.schemas.pipeline import PipelineCreate, PipelineRunCreate
from app.services.orchestrator import Orchestrator
from typing import List
import os
import shutil

router = APIRouter()
orchestrator = Orchestrator()

@router.post("/upload-csv")
async def upload_csv_and_create_pipeline(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload CSV file and create a sample pipeline based on it"""
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Create uploads directory if it doesn't exist
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save the uploaded file
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Create pipeline based on the uploaded CSV
        pipeline_id = orchestrator.create_pipeline_from_csv(db, file_path, file.filename)
        
        return {
            "message": "CSV uploaded and pipeline created successfully",
            "pipeline_id": pipeline_id,
            "file_path": file_path,
            "filename": file.filename
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pipelines")
def create_pipeline(pipeline: PipelineCreate, db: Session = Depends(get_db)):
    """Create a new pipeline"""
    print("Creating sample pipeline")
    created_pipeline = orchestrator.create_sample_pipeline(db)
    return created_pipeline

@router.get("/pipelines")
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
        
        return {
            "message": "Pipeline execution started",
            "pipeline_run_id": pipeline_run.id,
            "pipeline_id": pipeline_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipelines/{pipeline_id}/runs")
def get_pipeline_runs(pipeline_id: int, db: Session = Depends(get_db)):
    """Get all runs for a pipeline"""
    return db.query(PipelineRun).filter(PipelineRun.pipeline_id == pipeline_id).all()