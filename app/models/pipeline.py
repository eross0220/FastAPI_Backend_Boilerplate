from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, JSON, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base_class import Base
import enum

class PipelineStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class BlockType(str, enum.Enum):
    CSV_READER = "csv_reader"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    TOXICITY_DETECTION = "toxicity_detection"
    FILE_WRITER = "file_writer"

class BlockStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class Pipeline(Base):
    __tablename__ = "pipelines"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=False, index=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    blocks = relationship("Block", back_populates="pipeline")
    runs = relationship("PipelineRun", back_populates="pipeline")

class Block(Base):
    __tablename__ = "blocks"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    name = Column(String)
    block_type = Column(Enum(BlockType))
    config = Column(JSON)
    order = Column(Integer)
    
    pipeline = relationship("Pipeline", back_populates="blocks")
    dependencies = relationship("BlockDependency", foreign_keys="BlockDependency.block_id")

class BlockDependency(Base):
    __tablename__ = "block_dependencies"
    
    id = Column(Integer, primary_key=True, index=True)
    block_id = Column(Integer, ForeignKey("blocks.id"))
    depends_on_id = Column(Integer, ForeignKey("blocks.id"))

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    status = Column(Enum(PipelineStatus), default=PipelineStatus.QUEUED)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    pipeline = relationship("Pipeline", back_populates="runs")
    block_runs = relationship("BlockRun", back_populates="pipeline_run")

class BlockRun(Base):
    __tablename__ = "block_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_run_id = Column(Integer, ForeignKey("pipeline_runs.id"))
    block_id = Column(Integer, ForeignKey("blocks.id"))
    status = Column(Enum(BlockStatus), default=BlockStatus.PENDING)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    input_data = Column(JSON)
    output_data = Column(JSON)
    error_message = Column(Text)
    
    pipeline_run = relationship("PipelineRun", back_populates="block_runs")
    block = relationship("Block")

class Artifact(Base):
    __tablename__ = "artifacts"
    
    id = Column(Integer, primary_key=True, index=True)
    block_run_id = Column(Integer, ForeignKey("block_runs.id"))
    name = Column(String)
    file_path = Column(String)
    artifact_metadata = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())