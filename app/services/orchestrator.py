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
            pubsub.subscribe("block_completion_events", "data_flow_events")
            
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
        
        if event_type in ["block_completed", "block_failed"]:
            self._handle_block_completion_event(event_data)
        elif event_type == "data_ready":
            self._handle_data_ready_event(event_data)
    
    def _handle_block_completion_event(self, event_data: dict):
        """Handle block completion event from Redis"""
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
    
    def _handle_data_ready_event(self, event_data: dict):
        """Handle data ready event for next blocks"""
        source_block_run_id = event_data.get("source_block_run_id")
        data_type = event_data.get("data_type")
        data = event_data.get("data", {})
        target_blocks = event_data.get("target_blocks", [])
        
        print(f"Data ready: {data_type} from block {source_block_run_id} for blocks {target_blocks}")
        
        # Update configs for target blocks with the new data
        from app.database.session import SessionLocal
        db = SessionLocal()
        
        try:
            self._update_block_configs_with_data(db, source_block_run_id, data_type, data, target_blocks)
        finally:
            db.close()
    
    def _update_block_configs_with_data(self, db: Session, source_block_run_id: int, data_type: str, data: dict, target_blocks: list):
        """Update block configs with data from previous blocks"""
        # Get the source block run to find the pipeline run
        source_block_run = db.query(BlockRun).filter(BlockRun.id == source_block_run_id).first()
        if not source_block_run:
            return
        
        pipeline_run_id = source_block_run.pipeline_run_id
        
        # Find target blocks in the same pipeline run
        for target_block_type in target_blocks:
            # Find the block run for this block type
            target_block_run = db.query(BlockRun).join(Block).filter(
                BlockRun.pipeline_run_id == pipeline_run_id,
                Block.block_type == target_block_type
            ).first()
            
            if target_block_run and target_block_run.status == BlockStatus.PENDING:
                # Update the block config with the new data
                block = db.query(Block).filter(Block.id == target_block_run.block_id).first()
                if block:
                    # Merge existing config with new data
                    updated_config = block.config.copy() if block.config else {}
                    
                    # Add data based on data type
                    if data_type == "csv_data":
                        updated_config["texts"] = data.get("texts", [])
                        updated_config["csv_data"] = data
                    elif data_type == "sentiment_data":
                        updated_config["sentiment_data"] = data
                        updated_config["texts"] = data.get("texts", [])
                    elif data_type == "toxicity_data":
                        updated_config["toxicity_data"] = data
                        updated_config["texts"] = data.get("texts", [])
                    
                    # Update the block config
                    block.config = updated_config
                    db.commit()
                    
                    print(f"Updated config for {target_block_type} block with {data_type} data")
    
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
        db.commit()  # Commit here to get the IDs
        
        # Now the IDs will be available
        print(f"CSV_Reader Block id: {csv_block.id}")
        print(f"Sentiment_Analysis Block id: {sentiment_block.id}")
        print(f"Toxicity_Detection Block id: {toxicity_block.id}")
        print(f"File_Writer_Sentiment Block id: {file_writer_sentiment.id}")
        print(f"File_Writer_Toxicity Block id: {file_writer_toxicity.id}")
        
        # Create dependencies with valid IDs
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

        print(f"========== Pipeline Started: {pipeline_run.id} ==========")
        
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
        print(f"/* Ready Blocks Count: {len(ready_blocks)} */")
        
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