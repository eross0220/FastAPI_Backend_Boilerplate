from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models.pipeline import Pipeline, PipelineRun, Block, BlockRun, BlockStatus, PipelineStatus, BlockDependency
from app.core.kafka_client import KafkaClient
from datetime import datetime
import json
import os
import threading
import time
from rq import Queue
from redis import Redis
from workers.universal_worker import process_task
import pandas as pd
from app.models.pipeline import BlockType

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
        
        # Start Redis Pub/Sub consumer for block completion events
        self._start_redis_consumer()
    
    def _start_redis_consumer(self):
        """Start Redis Pub/Sub consumer to listen for block completion events"""
        def consume_block_events():
            pubsub = self.redis_conn.pubsub()
            pubsub.subscribe("block_completion_events")
            
            print("Started Redis consumer for block completion and data flow events")
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        event_data = json.loads(message['data'])
                        print(f"Received Redis event: {event_data.get('event_type')}")
                        
                        # Handle the event
                        self._handle_redis_event(event_data)
                        
                    except Exception as e:
                        print(f"Error processing Redis event: {e}")
        
        # Start consumer in background thread
        thread = threading.Thread(target=consume_block_events, daemon=True)
        thread.start()
    
    def _handle_redis_event(self, event_data: dict):
        """Handle events from Redis Pub/Sub"""
        event_type = event_data.get("event_type")
        print(f"*********Redis Event type***********: {event_type}")
        
        if event_type in ["block_completed", "block_failed"]:
            self._handle_block_completion_event(event_data)
        # elif event_type == "data_ready":
        #     self._handle_data_ready_event(event_data)
    
    def _handle_block_completion_event(self, event_data: dict):
        """Handle block completion event from Redis"""
        print(f"*********Redis Event data***********: {event_data}")
        block_run_id = event_data.get("block_run_id")
        success = event_data.get("success", False)
        result_data = event_data.get("result_data", {})
        
        # Update database and trigger next blocks
        from app.database.session import SessionLocal
        db = SessionLocal()
        
        try:
            # Check if block is already completed to prevent duplicate processing
            block_run = db.query(BlockRun).filter(BlockRun.id == block_run_id).first()
            if block_run and block_run.status == BlockStatus.COMPLETED:
                print(f"Block {block_run_id} already completed, skipping duplicate event")
                return
                
            self.handle_block_completion(db, block_run_id, result_data, success)
        finally:
            db.close()
    
    # def _handle_data_ready_event(self, event_data: dict):
    #     print(f"*********Data ready event***********: {event_data}")
    #     """Handle data ready event for next blocks"""
    #     source_block_run_id = event_data.get("source_block_run_id")
    #     data_type = event_data.get("data_type")
    #     data = event_data.get("data", {})
    #     target_blocks = event_data.get("target_blocks", [])
        
    #     print(f"Data ready: {data_type} from block {source_block_run_id} for blocks {target_blocks}")
        
    #     # Update configs for target blocks with the new data
    #     from app.database.session import SessionLocal
    #     db = SessionLocal()
        
    #     try:
    #         self._update_block_configs_with_data(db, source_block_run_id, data_type, data, target_blocks)
    #     finally:
    #         db.close()
    
    def _update_block_configs_with_data(self, db: Session, source_block_run_id: int, data_type: str, data: dict, target_blocks: list):
        """Update block configs with data from previous blocks"""
        print(f"🔄 Updating block configs with {data_type} data from block {source_block_run_id}")
        print(f"📤 Target blocks: {target_blocks}")
        print(f"📊 Data keys: {list(data.keys())}")
        
        # Get the source block run to find the pipeline run
        source_block_run = db.query(BlockRun).filter(BlockRun.id == source_block_run_id).first()
        if not source_block_run:
            print(f"❌ Source block run {source_block_run_id} not found")
            return
        
        pipeline_run_id = source_block_run.pipeline_run_id
        print(f"️  Pipeline run ID: {pipeline_run_id}")
        
        # Find target blocks in the same pipeline run
        for target_block_type in target_blocks:
            print(f"🎯 Looking for {target_block_type} block...")
            
            # Handle file writer types specifically
            if target_block_type == "file_writer":
                # Find file writers based on data type
                if data_type == "sentiment_data":
                    target_block_name = "File Writer (Sentiment)"
                elif data_type == "toxicity_data":
                    target_block_name = "File Writer (Toxicity)"
                else:
                    target_block_name = "File Writer"
                
                # Find by name instead of block type
                target_block_run = db.query(BlockRun).join(Block).filter(
                    BlockRun.pipeline_run_id == pipeline_run_id,
                    Block.name == target_block_name
                ).first()
            else:
                # For non-file-writer blocks, use block type
                target_block_run = db.query(BlockRun).join(Block).filter(
                    BlockRun.pipeline_run_id == pipeline_run_id,
                    Block.block_type == BlockType(target_block_type)
                ).first()
            
            print(f"*********Target block run***********: {target_block_run}, target_block_run.status: {target_block_run.status}, {target_block_run.block.name}")
            
            if target_block_run and target_block_run.status == BlockStatus.PENDING:
                print(f"✅ Found {target_block_type} block run {target_block_run.id}")
                
                # Update the block config with the new data
                block = db.query(Block).filter(Block.id == target_block_run.block_id).first()
                if block:
                    # Merge existing config with new data
                    updated_config = block.config.copy() if block.config else {}
                    
                    # Add data based on data type
                    if data_type == "csv_data":
                        updated_config["texts"] = data.get("texts", [])
                        updated_config["csv_data"] = data
                        print(f"📝 Added {len(data.get('texts', []))} texts to {target_block_type} config")
                    elif data_type == "sentiment_data":
                        updated_config["sentiment_data"] = data
                        updated_config["texts"] = data.get("texts", [])
                        updated_config["input_data"] = data.get("sentiments_results", [])
                        print(f"📝 Added sentiment data to {target_block_type} config")
                    elif data_type == "toxicity_data":
                        updated_config["toxicity_data"] = data
                        updated_config["texts"] = data.get("texts", [])
                        updated_config["input_data"] = data.get("toxicity_results", [])
                        print(f"📝 Added toxicity data to {target_block_type} config")
                    
                    # Update the block config
                    block.config = updated_config
                    db.commit()
                    
                    print(f"✅ Updated config for {target_block_type} block with {data_type} data")
                    print(f" Config keys: {list(updated_config.keys())}")
            else:
                print(f"⚠️  {target_block_type} block not found or not pending")
    
    def create_sample_pipeline(self, db: Session):
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
            block_type=BlockType.CSV_READER,
            config={"file_path": "./data/sample.csv", "purpose": "input_data"},
            order=1
        )
        
        sentiment_block = Block(
            pipeline_id=pipeline.id,
            name="Sentiment Analysis",
            block_type=BlockType.SENTIMENT_ANALYSIS,
            config={"purpose": "sentiment_processing"},
            order=2
        )
        
        toxicity_block = Block(
            pipeline_id=pipeline.id,
            name="Toxicity Detection",
            block_type=BlockType.TOXICITY_DETECTION,
            config={"purpose": "toxicity_processing"},
            order=3
        )
        
        file_writer_sentiment = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Sentiment)",
            block_type=BlockType.FILE_WRITER,
            config={
                "purpose": "sentiment_output",
                "output_type": "sentiment_results",
                "file_prefix": "sentiment_"
            },
            order=4
        )
        
        file_writer_toxicity = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Toxicity)",
            block_type=BlockType.FILE_WRITER,
            config={
                "purpose": "toxicity_output", 
                "output_type": "toxicity_results",
                "file_prefix": "toxicity_"
            },
            order=5
        )
        
        db.add_all([csv_block, sentiment_block, toxicity_block, file_writer_sentiment, file_writer_toxicity])
        db.commit()
        
        # Create dependencies
        sentiment_dep = BlockDependency(block_id=sentiment_block.id, depends_on_id=csv_block.id)
        toxicity_dep = BlockDependency(block_id=toxicity_block.id, depends_on_id=csv_block.id)
        file_sentiment_dep = BlockDependency(block_id=file_writer_sentiment.id, depends_on_id=sentiment_block.id)
        file_toxicity_dep = BlockDependency(block_id=file_writer_toxicity.id, depends_on_id=toxicity_block.id)
        
        db.add_all([sentiment_dep, toxicity_dep, file_sentiment_dep, file_toxicity_dep])
        db.commit()
        
        return pipeline.id
    
    def _create_file_writer_blocks(self, pipeline: Pipeline, db: Session):
        file_writer_toxicity = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Toxicity)",
            block_type=BlockType.FILE_WRITER,
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
        print(f"*********Ready blocks***********: {len(ready_blocks)}")        
        # Dispatch ready blocks to RQ queue
        for block_run in ready_blocks:
            self._dispatch_block_to_rq_queue(db, block_run)
    
    def _find_ready_blocks(self, db: Session, pipeline_run_id: int, block_dependencies: Dict[int, List[int]]) -> List[BlockRun]:
        """Find blocks that are ready to run (dependencies satisfied)"""
        ready_blocks = []
        
        for block_id, dependencies in block_dependencies.items():
            block_run = self._get_block_run(db, pipeline_run_id, block_id)
            
            # Skip if block is already running, completed, or failed
            if block_run.status in [BlockStatus.RUNNING, BlockStatus.COMPLETED, BlockStatus.FAILED]:
                continue

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
        """Dispatch a block to RQ queue with enhanced config"""
        # Double-check block status before dispatching
        if block_run.status != BlockStatus.PENDING:
            print(f"Block {block_run.id} is not pending (status: {block_run.status}), skipping dispatch")
            return
        block = db.query(Block).filter(Block.id == block_run.block_id).first()
        
        
        # Emit block started event
        self.kafka_client.publish_event(
            "block_events",
            {
                "event_type": "block_started",
                "block_run_id": block_run.id,
                "block_type": block.block_type.value,
                "block_name": block.name,
                "block_description": f"{block.block_type.value} for {self._get_block_purpose(block)}",
                "block_config": block.config,
                "block_purpose": self._get_block_purpose(block),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Enhanced config with block_run_id for tracking
        enhanced_config = block.config.copy() if block.config else {}
        enhanced_config["block_run_id"] = block_run.id
        
        # Dispatch to single RQ queue - any worker can pick it up
        job = self.task_queue.enqueue_call(
            func=process_task,
            args=(block_run.id, block.block_type.value, enhanced_config),
            result_ttl=5000
        )
        
        print(f"Dispatched {block.block_type.value} to queue, job_id: {job.get_id()}")

        # Update block run status
        block_run.status = BlockStatus.RUNNING
        block_run.started_at = datetime.utcnow()
        db.commit()

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
                "block_type":block_run.block.block_type.value,
                "block_name": block_run.block.name,
                "block_description": f"{block_run.block.block_type.value} for {self._get_block_purpose(block_run.block)}",
                "block_config": block_run.block.config,
                "block_purpose": self._get_block_purpose(block_run.block),
                "success": success,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        # ✅ FIX: Process data flow BEFORE dispatching next blocks
        print(f"*********Block CompletionResult data***********: {result_data}")
        if success and "data_type" in result_data.get("result", {}):
            result_data_result = result_data.get("result", {})
            print(f"🔄 Processing data flow before dispatching next blocks")
            
            # Update configs for next blocks
            self._update_block_configs_with_data(
                db, 
                block_run_id, 
                result_data_result.get("data_type"),
                result_data_result,
                result_data_result.get("next_blocks", [])
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

    def _get_block_purpose(self, block: Block) -> str:
        """Get a human-readable description of what this block does"""
        if block.block_type.value == BlockType.FILE_WRITER:
            # Check the block name to determine purpose
            if "sentiment" in block.name.lower():
                return "sentiment analysis results"
            elif "toxicity" in block.name.lower():
                return "toxicity detection results"
            else:
                return "general output"
        elif block.block_type.value == BlockType.CSV_READER:
            return "CSV data input"
        elif block.block_type.value == BlockType.SENTIMENT_ANALYSIS:
            return "sentiment analysis processing"
        elif block.block_type.value == BlockType.TOXICITY_DETECTION:
            return "toxicity detection processing"
        else:
            return "data processing"

    def create_pipeline_from_csv(self, db: Session, csv_file_path: str, filename: str) -> int:
        """Create a pipeline based on uploaded CSV file"""
        # Create pipeline
        pipeline = Pipeline(
            name=f"Pipeline for {filename}",
            description=f"CSV → Sentiment Analysis → Toxicity Detection → File Writers"
        )
        db.add(pipeline)
        db.commit()
        
        # Create blocks
        csv_block = Block(
            pipeline_id=pipeline.id,
            name="CSV Reader",
            block_type=BlockType.CSV_READER,
            config={"file_path": csv_file_path, "purpose": "input_data"},
            order=1
        )
        
        sentiment_block = Block(
            pipeline_id=pipeline.id,
            name="Sentiment Analysis",
            block_type=BlockType.SENTIMENT_ANALYSIS,
            config={"purpose": "sentiment_processing"},
            order=2
        )
        
        toxicity_block = Block(
            pipeline_id=pipeline.id,
            name="Toxicity Detection",
            block_type=BlockType.TOXICITY_DETECTION,
            config={"purpose": "toxicity_processing"},
            order=3
        )
        
        file_writer_sentiment = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Sentiment)",
            block_type=BlockType.FILE_WRITER,
            config={
                "purpose": "sentiment_output",
                "output_type": "sentiment_results",
                "file_prefix": "sentiment_"
            },
            order=4
        )
        
        file_writer_toxicity = Block(
            pipeline_id=pipeline.id,
            name="File Writer (Toxicity)",
            block_type=BlockType.FILE_WRITER,
            config={
                "purpose": "toxicity_output", 
                "output_type": "toxicity_results",
                "file_prefix": "toxicity_"
            },
            order=5
        )
        
        db.add_all([csv_block, sentiment_block, toxicity_block, file_writer_sentiment, file_writer_toxicity])
        db.commit()
        
        # Create dependencies
        sentiment_dep = BlockDependency(block_id=sentiment_block.id, depends_on_id=csv_block.id)
        toxicity_dep = BlockDependency(block_id=toxicity_block.id, depends_on_id=csv_block.id)
        file_sentiment_dep = BlockDependency(block_id=file_writer_sentiment.id, depends_on_id=sentiment_block.id)
        file_toxicity_dep = BlockDependency(block_id=file_writer_toxicity.id, depends_on_id=toxicity_block.id)
        
        db.add_all([sentiment_dep, toxicity_dep, file_sentiment_dep, file_toxicity_dep])
        db.commit()
        
        print(f"Created pipeline {pipeline.id} for CSV file: {filename}")
        return pipeline.id