import pandas as pd
import os
from typing import Dict, Any
from pathlib import Path
import redis
import json
from datetime import datetime

class WorkerRedisClient:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_client = redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=0,
            decode_responses=True
        )
    
    def publish_block_completion(self, block_run_id: int, result_data: dict, success: bool):
        """Publish block completion event to Redis channel"""
        event = {
            "event_type": "block_completed" if success else "block_failed",
            "block_run_id": block_run_id,
            "success": success,
            "result_data": result_data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            # Publish to Redis channel
            self.redis_client.publish("block_completion_events", json.dumps(event))
            print(f"Published block completion event for block_run_id: {block_run_id}")
            
            # Also store in Redis for persistence (optional)
            event_key = f"block_event:{block_run_id}:{datetime.utcnow().timestamp()}"
            self.redis_client.setex(event_key, 3600, json.dumps(event))  # Expire in 1 hour
            
        except Exception as e:
            print(f"Error publishing to Redis: {e}")
    
    def publish_data_ready(self, block_run_id: int, data_type: str, data: dict, target_blocks: list):
        """Publish data ready event for next blocks"""
        event = {
            "event_type": "data_ready",
            "source_block_run_id": block_run_id,
            "data_type": data_type,
            "data": data,
            "target_blocks": target_blocks,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            self.redis_client.publish("data_flow_events", json.dumps(event))
            print(f"Published data ready event for {data_type} to {target_blocks}")
        except Exception as e:
            print(f"Error publishing data ready event: {e}")
redis_client = WorkerRedisClient()

def process_task(block_run_id: int, block_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Universal worker that can handle any task type"""
    try:
        print(f"Processing {block_type} for block_run_id: {block_run_id}")
        
        if block_type == "csv_reader":
            result = _process_csv_reader(block_run_id, config)
        elif block_type == "sentiment_analysis":
            result = _process_sentiment_analysis(block_run_id, config)
        elif block_type == "toxicity_detection":
            result = _process_toxicity_detection(block_run_id, config)
        elif block_type == "file_writer":
            result = _process_file_writer(block_run_id, config)
        else:
            raise ValueError(f"Unknown block type: {block_type}")
        
        # Publish completion event
        redis_client.publish_block_completion(block_run_id, result, success=True)
        
        # Publish data ready event for next blocks
        if result.get("success") and "data_type" in result.get("result", {}):
            result_data = result.get("result", {})
            redis_client.publish_data_ready(
                block_run_id=block_run_id,
                data_type=result_data.get("data_type"),
                data=result_data,
                target_blocks=result_data.get("next_blocks", [])
            )
        
        return result
            
    except Exception as e:
        error_result = {"success": False, "error": str(e), "block_run_id": block_run_id}
        print(f"Task failed for block_run_id: {block_run_id}: {e}")
        
        # Publish failure event
        redis_client.publish_block_completion(block_run_id, error_result, success=False)
        return error_result

def _process_csv_reader(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process CSV Reader tasks - MOCKUP VERSION"""
    print(f"Processing CSV Reader for block_run_id: {block_run_id}")
    file_path = config.get("file_path", "./data/sample.csv")
    
    # Mock CSV data - replace with actual CSV reading logic
    mock_data = [
        {"id": 1, "text": "This is a great product!", "user": "user1"},
        {"id": 2, "text": "I'm not satisfied with the service.", "user": "user2"},
        {"id": 3, "text": "The quality is okay, nothing special.", "user": "user3"},
        {"id": 4, "text": "Absolutely love it! Best purchase ever.", "user": "user4"},
        {"id": 5, "text": "Terrible experience, would not recommend.", "user": "user5"}
    ]
    
    # Enhanced result structure with data flow information
    result = {
        "rows": mock_data,
        "columns": ["id", "text", "user"],
        "row_count": len(mock_data),
        "file_path": file_path,
        "data_type": "csv_data",
        "next_blocks": ["sentiment_analysis", "toxicity_detection"],
        "texts": [row["text"] for row in mock_data]  # Extract texts for next blocks
    }
    
    print(f"CSV Reader Completed")
    return {"success": True, "result": result}

def _process_sentiment_analysis(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process Sentiment Analysis tasks - MOCKUP VERSION"""
    print(f"Processing Sentiment Analysis for block_run_id: {block_run_id}")
    
    # Get texts from config (should be populated by orchestrator)
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
    
    result = {
        "sentiments": results,
        "data_type": "sentiment_data",
        "next_blocks": ["file_writer"],
        "texts": texts  # Pass through for file writer
    }
    
    print(f"Sentiment Analysis Completed")
    return {"success": True, "result": result}

def _process_toxicity_detection(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process Toxicity Detection tasks - MOCKUP VERSION"""
    print(f"Processing Toxicity Detection for block_run_id: {block_run_id}")
    
    # Get texts from config (should be populated by orchestrator)
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
    
    result = {
        "toxicity_results": results,
        "data_type": "toxicity_data",
        "next_blocks": ["file_writer"],
        "texts": texts  # Pass through for file writer
    }
    
    print(f"Toxicity Detection Completed")
    return {"success": True, "result": result}

def _process_file_writer(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process File Writer tasks - MOCKUP VERSION"""
    print(f"Processing File Writer for block_run_id: {block_run_id}")
    
    # Get input data from config (should be populated by orchestrator)
    input_data = config.get("input_data", [])
    output_format = config.get("output_format", "json")
    output_path = config.get("output_path", f"./output/block_run_{block_run_id}.{output_format}")
    
    # Mock file writing operation
    mock_file_info = {
        "output_path": output_path,
        "file_size": "2.5 KB",
        "records_written": len(input_data) if input_data else 5,
        "format": output_format,
        "status": "completed"
    }
    
    result = {
        "file_info": mock_file_info,
        "data_type": "file_output",
        "next_blocks": [],  # End of pipeline
        "input_data": input_data
    }
    
    print(f"File Writer Completed")
    return {"success": True, "result": result}