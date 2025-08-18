from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models.pipeline import Pipeline, PipelineRun, Block, BlockRun, BlockStatus, PipelineStatus, BlockDependency
from app.core.kafka_client import KafkaClient
from datetime import datetime
import json
import os
from rq import Queue
from redis import Redis
from workers.universal_worker import process_task

class Orchestrator:
    def __init__(self):
        # Redis connection for RQ
        self.redis_conn = Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379))
        )
        
        # Single RQ queue for all tasks
        self.task_queue = Queue('pipeline_tasks', connection=self.redis_conn)
        
        self.kafka_client = KafkaClient()
    
    def create_sample_pipeline(self, db:Session):

         # Create pipeline
        pipeline = Pipeline(
            name="Sample Pipeline",
            description="CSV → LLM → File Writer"
        )
        db.add(pipeline)
        db.commit()
        
        # Create blocks
        csv_block = Block(
            pipeline_id=pipeline.id,
            name="CSV Reader",
            block_type="csv_reader",
            config={"file_path": "./data/sample.csv"},
            order=1
        )
        
        sentiment_block = Block(
            pipeline_id=pipeline.id,
            name="Sentiment Analysis",
            block_type="sentiment_analysis",
            config={},  # Will be populated with CSV data
            order=2
        )
        
        toxicity_block = Block(
            pipeline_id=pipeline.id,
            name="Toxicity Detection",
            block_type="toxicity_detection",
            config={},  # Will be populated with CSV data
            order=3
        )
        
        file_writer_sentiment = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Sentiment)",
            block_type="file_writer",
            config={},  # Will be populated with sentiment results
            order=4
        )
        
        file_writer_toxicity = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Toxicity)",
            block_type="file_writer",
            config={},  # Will be populated with toxicity results
            order=5
        )
        
        db.add_all([csv_block, sentiment_block, toxicity_block, file_writer_sentiment, file_writer_toxicity])
        db.commit()
        
        # Create dependencies
        print(f"@@@@@ Sentiment block id: {sentiment_block.id}")
        print(f"@@@@@ CSV block id: {csv_block.id}")
        print(f"@@@@@ file_writer_sentiment.id: {file_writer_sentiment.id}")
        print(f"@@@@@ file_writer_toxicity.id: {file_writer_toxicity.id}")

        sentiment_dep = BlockDependency(block_id=sentiment_block.id, depends_on_id=csv_block.id)
        toxicity_dep = BlockDependency(block_id=toxicity_block.id, depends_on_id=csv_block.id)
        file_sentiment_dep = BlockDependency(block_id=file_writer_sentiment.id, depends_on_id=sentiment_block.id)
        file_toxicity_dep = BlockDependency(block_id=file_writer_toxicity.id, depends_on_id=toxicity_block.id)
        
        db.add_all([sentiment_dep, toxicity_dep, file_sentiment_dep, file_toxicity_dep])
        db.commit()
        
        return pipeline.id

    
    def _process_csv_reader(config: Dict[str, Any]) -> Dict[str, Any]:
        """Process CSV Reader tasks - MOCKUP VERSION"""
        print(f"Processing CSV Reader for block_run_id: {config.get('block_run_id')}")
        file_path = config.get("file_path", "./data/sample.csv")
        
        # Mock CSV data - replace with actual CSV reading logic
        mock_data = [
            {"id": 1, "text": "This is a great product!", "user": "user1"},
            {"id": 2, "text": "I'm not satisfied with the service.", "user": "user2"},
            {"id": 3, "text": "The quality is okay, nothing special.", "user": "user3"},
            {"id": 4, "text": "Absolutely love it! Best purchase ever.", "user": "user4"},
            {"id": 5, "text": "Terrible experience, would not recommend.", "user": "user5"}
        ]
        
        # Mock result structure
        result = {
            "rows": mock_data,
            "columns": ["id", "text", "user"],
            "row_count": len(mock_data),
            "file_path": file_path
        }
        
        print(f"CSV Reader completed, processed {len(mock_data)} rows (MOCKUP)")
        return {"success": True, "result": result}

    def _process_sentiment_analysis(config: Dict[str, Any]) -> Dict[str, Any]:
        """Process Sentiment Analysis tasks - MOCKUP VERSION"""
        print(f"Processing Sentiment Analysis for block_run_id: {config.get('block_run_id')}")
        
        # Mock input texts - replace with actual data from previous block
        texts = config.get("texts", [
            "This is a great product!",
            "I'm not satisfied with the service.",
            "The quality is okay, nothing special.",
            "Absolutely love it! Best purchase ever.",
            "Terrible experience, would not recommend."
        ])
        
        # Mock sentiment analysis results
        mock_sentiments = ["POSITIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "NEGATIVE"]
        mock_scores = [5, 0, 2.5, 5, 0]
        
        results = []
        for i, text in enumerate(texts):
            results.append({
                "text": text,
                "sentiment": mock_sentiments[i],
                "score": mock_scores[i]
            })
        
        print(f"Sentiment Analysis completed, processed {len(texts)} texts (MOCKUP)")
        return {"success": True, "result": results}

    def _process_toxicity_detection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Process Toxicity Detection tasks - MOCKUP VERSION"""
        print(f"Processing Toxicity Detection for block_run_id: {config.get('block_run_id')}")
        
        # Mock input texts - replace with actual data from previous block
        texts = config.get("texts", [
            "This is a great product!",
            "I'm not satisfied with the service.",
            "The quality is okay, nothing special.",
            "Absolutely love it! Best purchase ever.",
            "Terrible experience, would not recommend."
        ])
        
        # Mock toxicity detection results
        mock_toxicity = ["NON_TOXIC", "NON_TOXIC", "NON_TOXIC", "NON_TOXIC", "NON_TOXIC"]
        mock_scores = [0, 0, 0, 0, 0]
        
        results = []
        for i, text in enumerate(texts):
            results.append({
                "text": text,
                "toxicity": mock_toxicity[i],
                "score": mock_scores[i]
            })
        
        print(f"Toxicity Detection completed, processed {len(texts)} texts (MOCKUP)")
        return {"success": True, "result": results}

    def _process_file_writer(config: Dict[str, Any]) -> Dict[str, Any]:
        """Process File Writer tasks - MOCKUP VERSION"""
        print(f"Processing File Writer for block_run_id: {config.get('block_run_id')}")
        
        # Mock input data - replace with actual data from previous block
        input_data = config.get("input_data", [])
        output_format = config.get("output_format", "json")
        output_path = config.get("output_path", f"./output/block_run_{config.get('block_run_id')}.{output_format}")
        
        # Mock file writing operation
        mock_file_info = {
            "output_path": output_path,
            "file_size": "2.5 KB",
            "records_written": len(input_data) if input_data else 5,
            "format": output_format,
            "status": "completed"
        }
        
        print(f"File Writer completed, wrote to {output_path} (MOCKUP)")
        return {"success": True, "result": mock_file_info}
        
    def _create_file_writer_blocks(self, pipeline: Pipeline, db: Session):
        file_writer_toxicity = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Toxicity)",
            block_type="file_writer",
            config={},  # Will be populated with toxicity results
            order=5
        )
        
        db.add_all([csv_block, sentiment_block, toxicity_block, file_writer_sentiment, file_writer_toxicity])
        
        # Create dependencies
        sentiment_dep = BlockDependency(block_id=sentiment_block.id, depends_on_id=csv_block.id)
        toxicity_dep = BlockDependency(block_id=toxicity_block.id, depends_on_id=csv_block.id)
        file_sentiment_dep = BlockDependency(block_id=file_writer_sentiment.id, depends_on_id=sentiment_block.id)
        file_toxicity_dep = BlockDependency(block_id=file_writer_toxicity.id, depends_on_id=toxicity_block.id)
        
        db.add_all([sentiment_dep, toxicity_dep, file_sentiment_dep, file_toxicity_dep])
        db.commit()
        
        return pipeline.id

    def create_pipeline_run(self, db: Session, pipeline_id: int) -> PipelineRun:
        """Create a new pipeline run"""
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if not pipeline:
            raise ValueError("Pipeline not found")
        
        # Create pipeline run
        pipeline_run = PipelineRun(
            pipeline_id=pipeline_id,
            status=PipelineStatus.QUEUED
        )
        db.add(pipeline_run)
        db.commit()
        db.refresh(pipeline_run)
        
        # Create block runs for all blocks
        blocks = db.query(Block).filter(Block.pipeline_id == pipeline_id).order_by(Block.order).all()
        block_runs = []
        
        for block in blocks:
            block_run = BlockRun(
                pipeline_run_id=pipeline_run.id,
                block_id=block.id,
                status=BlockStatus.PENDING
            )
            block_runs.append(block_run)
        
        db.add_all(block_runs)
        db.commit()
        
        # Emit pipeline started event
        self.kafka_client.publish_event(
            "pipeline_events",
            {
                "event_type": "pipeline_started",
                "pipeline_run_id": pipeline_run.id,
                "pipeline_id": pipeline_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return pipeline_run
    
    def resolve_dag_and_dispatch(self, db: Session, pipeline_run_id: int):
        """Resolve DAG dependencies and dispatch ready tasks using RQ"""
        pipeline_run = db.query(PipelineRun).filter(PipelineRun.id == pipeline_run_id).first()
        if not pipeline_run:
            return
        
        # Get all blocks and their dependencies
        blocks = db.query(Block).filter(Block.pipeline_id == pipeline_run.pipeline_id).all()
        block_dependencies = {}
        
        for block in blocks:
            dependencies = db.query(BlockDependency).filter(
                BlockDependency.block_id == block.id
            ).all()
            block_dependencies[block.id] = [dep.depends_on_id for dep in dependencies]
        
        # Find ready blocks (no dependencies or all dependencies completed)
        ready_blocks = self._find_ready_blocks(db, pipeline_run_id, block_dependencies)
        print(f"@@@@@ Ready blocks: {len(ready_blocks)}")
        # Dispatch ready blocks to RQ queue
        for block_run in ready_blocks:
            self._dispatch_block_to_rq_queue(db, block_run)
    
    def _find_ready_blocks(self, db: Session, pipeline_run_id: int, block_dependencies: Dict[int, List[int]]) -> List[BlockRun]:
        """Find blocks that are ready to run (dependencies satisfied)"""
        ready_blocks = []
        
        for block_id, dependencies in block_dependencies.items():
            if not dependencies:  # No dependencies
                ready_blocks.append(self._get_block_run(db, pipeline_run_id, block_id))
            else:
                # Check if all dependencies are completed
                all_completed = True
                for dep_id in dependencies:
                    dep_run = self._get_block_run(db, pipeline_run_id, dep_id)
                    if dep_run.status != BlockStatus.COMPLETED:
                        all_completed = False
                        break
                
                if all_completed:
                    ready_blocks.append(self._get_block_run(db, pipeline_run_id, block_id))
        
        return ready_blocks
    
    def _get_block_run(self, db: Session, pipeline_run_id: int, block_id: int) -> BlockRun:
        """Get block run for a specific block in a pipeline run"""
        return db.query(BlockRun).filter(
            BlockRun.pipeline_run_id == pipeline_run_id,
            BlockRun.block_id == block_id
        ).first()
    
    def _dispatch_block_to_rq_queue(self, db: Session, block_run: BlockRun):
        """Dispatch a block to RQ queue"""
        block = db.query(Block).filter(Block.id == block_run.block_id).first()
        
        # Update block run status
        block_run.status = BlockStatus.RUNNING
        block_run.started_at = datetime.utcnow()
        db.commit()
        
        # Emit block started event
        self.kafka_client.publish_event(
            "block_events",
            {
                "event_type": "block_started",
                "block_run_id": block_run.id,
                "block_type": block.block_type.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Dispatch to single RQ queue - any worker can pick it up
        job = self.task_queue.enqueue_call(
            func=process_task,
            args=(block_run.id, block.block_type.value, block.config),
            result_ttl=5000
        )
        
        print(f"Dispatched {block.block_type.value} to queue, job_id: {job.get_id()}")
    
    def handle_block_completion(self, db: Session, block_run_id: int, result_data: dict, success: bool = True):
        """Handle completion of a block run"""
        block_run = db.query(BlockRun).filter(BlockRun.id == block_run_id).first()
        if not block_run:
            return
        
        if success:
            block_run.status = BlockStatus.COMPLETED
            block_run.output_data = result_data
            block_run.completed_at = datetime.utcnow()
        else:
            block_run.status = BlockStatus.FAILED
            block_run.error_message = result_data.get("error", "Unknown error")
            block_run.completed_at = datetime.utcnow()
        
        db.commit()
        
        # Emit block completion event
        self.kafka_client.publish_event(
            "block_events",
            {
                "event_type": "block_completed" if success else "block_failed",
                "block_run_id": block_run_id,
                "success": success,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Check if pipeline is complete or if we can dispatch more tasks
        self._check_pipeline_completion(db, block_run.pipeline_run_id)
        self.resolve_dag_and_dispatch(db, block_run.pipeline_run_id)
    
    def _check_pipeline_completion(self, db: Session, pipeline_run_id: int):
        """Check if pipeline run is complete"""
        pipeline_run = db.query(PipelineRun).filter(PipelineRun.id == pipeline_run_id).first()
        block_runs = db.query(BlockRun).filter(BlockRun.pipeline_run_id == pipeline_run_id).all()
        
        # Check if all blocks are completed
        all_completed = all(br.status == BlockStatus.COMPLETED for br in block_runs)
        any_failed = any(br.status == BlockStatus.FAILED for br in block_runs)
        
        if all_completed:
            pipeline_run.status = PipelineStatus.COMPLETED
            pipeline_run.completed_at = datetime.utcnow()
        elif any_failed:
            pipeline_run.status = PipelineStatus.FAILED
            pipeline_run.completed_at = datetime.utcnow()
        
        db.commit()
        
        # Emit pipeline completion event
        if all_completed or any_failed:
            self.kafka_client.publish_event(
                "pipeline_events",
                {
                    "event_type": "pipeline_completed" if all_completed else "pipeline_failed",
                    "pipeline_run_id": pipeline_run_id,
                    "success": all_completed,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )