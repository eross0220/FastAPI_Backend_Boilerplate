from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.models.pipeline import PipelineRun, BlockRun, BlockStatus
import os
from pathlib import Path
from app.models.pipeline import Block

router = APIRouter()

# Configure output directory (where your CSV files are saved)
OUTPUT_DIR = "/app/outputs"

@router.get("/pipeline/{pipeline_run_id}/files")
async def get_pipeline_files(pipeline_run_id: int, db: Session = Depends(get_db)):
    """Get all available files for a specific pipeline run"""
    try:
        # Get pipeline run
        pipeline_run = db.query(PipelineRun).filter(PipelineRun.id == pipeline_run_id).first()
        if not pipeline_run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        
        # Get file writer blocks that completed successfully
        file_writer_blocks = db.query(BlockRun).join(Block).filter(
            BlockRun.pipeline_run_id == pipeline_run_id,
            Block.block_type == "file_writer",
            BlockRun.status == BlockStatus.COMPLETED
        ).all()
        
        files = []
        for block_run in file_writer_blocks:
            if block_run.output_data and "result" in block_run.output_data:
                result = block_run.output_data["result"]
                if "file_info" in result:
                    file_info = result["file_info"]
                    files.append({
                        "block_name": block_run.block.name,
                        "filename": file_info.get("filename"),
                        "file_size": file_info.get("file_size"),
                        "records_written": file_info.get("records_written"),
                        "download_url": f"/api/v1/downloads/pipeline/{pipeline_run_id}/file/{file_info.get('filename')}"
                    })
        
        return {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_status": pipeline_run.status.value,
            "files": files,
            "total_files": len(files)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline files: {str(e)}")

@router.get("/pipeline/{pipeline_run_id}/file/{filename}")
async def download_pipeline_file(pipeline_run_id: int, filename: str, db: Session = Depends(get_db)):
    """Download a specific file from a pipeline run"""
    try:
        # Verify the file belongs to this pipeline run
        pipeline_run = db.query(PipelineRun).filter(PipelineRun.id == pipeline_run_id).first()
        if not pipeline_run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        
        # Check if file exists in output directory
        file_path = os.path.join(OUTPUT_DIR, filename)
        print(f"Download File path: {file_path}")
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")
        
        # Security check: ensure file is within output directory
        real_path = os.path.realpath(file_path)
        output_dir = os.path.realpath(OUTPUT_DIR)
        if not real_path.startswith(output_dir):
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Return file as download
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        print(f"❌ Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")
