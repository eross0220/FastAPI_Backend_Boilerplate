import pandas as pd
import os
from typing import Dict, Any
from pathlib import Path
import redis
import json
from datetime import datetime
import time
from openai import OpenAI
import enum
from dotenv import load_dotenv
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class BlockType(str, enum.Enum):
    CSV_READER = "csv_reader"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    TOXICITY_DETECTION = "toxicity_detection"
    FILE_WRITER = "file_writer"

def analyze_sentiment_with_openai(text: str) -> dict:
    """Analyze sentiment of a single text using OpenAI"""
    try:
        api_key = OPENAI_API_KEY
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        client = OpenAI(api_key=api_key)
        
        system_prompt = """You are an AI assistant that classifies the sentiment of short text messages.
            For each input message, respond with exactly one of: POSITIVE, NEGATIVE, or NEUTRAL.
            Respond only with the label, no explanations.
            """
                    
        user_prompt = f"Input Message: {text}\nSentiment:"
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=10,
            temperature=0.1,
        )
        
        sentiment = response.choices[0].message.content.strip().upper()
        
        # Validate sentiment response
        if sentiment not in ["POSITIVE", "NEGATIVE", "NEUTRAL"]:
            print(f"⚠️  Unexpected sentiment response: {sentiment}, defaulting to NEUTRAL")
            sentiment = "NEUTRAL"
    
        
        return {
            "text": text,
            "sentiment": sentiment,
            "confidence": 0.9
        }
        
    except Exception as e:
        print(f"⚠️  Error analyzing sentiment for text: {str(e)}")
        return {
            "text": text,
            "sentiment": "NEUTRAL",
            "score": 0.5,
            "confidence": 0.0,
            "error": str(e)
        }

def detect_toxicity_with_openai(text: str) -> dict:
    """Detect toxicity in a single text using OpenAI"""
    try:
        api_key = OPENAI_API_KEY
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        client = OpenAI(api_key=api_key)
        
        system_prompt = """You are an AI assistant that detects toxic content in text messages.
For each input message, respond with exactly one of: TOXIC or NON_TOXIC.
Respond only with the label, no explanations."""
        
        user_prompt = f"Input Message: {text}\nToxicity:"
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=10,
            temperature=0.1,
        )
        
        toxicity = response.choices[0].message.content.strip().upper()
        
        # Validate toxicity response
        if toxicity not in ["TOXIC", "NON_TOXIC"]:
            print(f"⚠️  Unexpected toxicity response: {toxicity}, defaulting to NON_TOXIC")
            toxicity = "NON_TOXIC"
        
        return {
            "text": text,
            "toxicity": toxicity,
        }
        
    except Exception as e:
        print(f"⚠️  Error detecting toxicity for text: {str(e)}")
        return {
            "text": text,
            "toxicity": "NON_TOXIC",
            "score": 0.1,
            "error": str(e)
        }


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
        print(f"*********Data ready event***********: {event}")
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
        
        if block_type == BlockType.CSV_READER:
            result = _process_csv_reader(block_run_id, config)
        elif block_type == BlockType.SENTIMENT_ANALYSIS:
            result = _process_sentiment_analysis(block_run_id, config)
        elif block_type == BlockType.TOXICITY_DETECTION:
            result = _process_toxicity_detection(block_run_id, config)
        elif block_type == BlockType.FILE_WRITER:
            result = _process_file_writer(block_run_id, config)
        else:
            raise ValueError(f"Unknown block type: {block_type}")
        
        success = result.get("success", False)
        redis_client.publish_block_completion(block_run_id, result, success=success)
        
        # # Publish data ready event for next blocks
        # if result.get("success") and "data_type" in result.get("result", {}):
        #     result_data = result.get("result", {})
        #     redis_client.publish_data_ready(
        #         block_run_id=block_run_id,
        #         data_type=result_data.get("data_type"),
        #         data=result_data,
        #         target_blocks=result_data.get("next_blocks", [])
        #     )
        
        return result
            
    except Exception as e:
        error_result = {"success": False, "error": str(e), "block_run_id": block_run_id}
        print(f"Task failed for block_run_id: {block_run_id}: {e}")
        
        # Publish failure event
        redis_client.publish_block_completion(block_run_id, error_result, success=False)
        return error_result

def _process_csv_reader(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process CSV Reader tasks - REAL CSV READING VERSION"""
    print(f"Processing CSV Reader for block_run_id: {block_run_id}")
    file_path = config.get("file_path", "./data/sample.csv")
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            error_msg = f"CSV file not found at path: {file_path}"
            print(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        print(f"📁 Reading CSV file from: {file_path}")
        
        # Read CSV file using pandas
        df = pd.read_csv(file_path)
        csv_data = df.to_dict('records')
        columns = df.columns.tolist()
        text_column = 'text' if 'text' in columns else columns[0]
        texts = df[text_column].astype(str).tolist()
        
        print(f"✅ Successfully read {len(csv_data)} rows with {len(columns)} columns")
        print(f"📊 Columns: {columns}")
        print(f"📝 Text column: {text_column}")
        
        # Enhanced result structure with real data flow information
        result = {
            "rows": csv_data,
            "columns": columns,
            "row_count": len(csv_data),
            "file_path": file_path,
            "data_type": "csv_data",
            "next_blocks": [BlockType.SENTIMENT_ANALYSIS.value, BlockType.TOXICITY_DETECTION.value],
            "texts": texts, # Extract texts for next blocks
            "text_column": text_column,
        }
        
        print(f"🚀 CSV Reader Completed - Ready to process {len(texts)} texts")
        return {"success": True, "result": result}
        
    except FileNotFoundError:
        error_msg = f"CSV file not found: {file_path}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
        
    except pd.errors.EmptyDataError:
        error_msg = f"CSV file is empty: {file_path}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
        
    except pd.errors.ParserError as e:
        error_msg = f"Error parsing CSV file: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
        
    except Exception as e:
        error_msg = f"Unexpected error reading CSV: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def _process_sentiment_analysis(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process Sentiment Analysis tasks using OpenAI API"""
    print(f"Processing Sentiment Analysis for block_run_id: {block_run_id}")
    
    # Get texts from config (should be populated by orchestrator)
    texts = config.get("texts", [])
    print(f"*********Sentiment Analysis: {texts}")
    
    if not texts:
        error_msg = "No texts provided for sentiment analysis"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    
    print(f"📝 Analyzing sentiment for {len(texts)} texts using OpenAI")
    
    try:
        results = []
        batch_size = 10  # Process in batches to avoid rate limits
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            print(f" Processing batch {i//batch_size + 1}: texts {i+1}-{min(i+batch_size, len(texts))}")
            
            for j, text in enumerate(batch_texts):
                try:
                    # Use the separate function
                    result = analyze_sentiment_with_openai(text)
                    results.append(result)
                    # results.append({
                    #     "text": text,
                    #     "sentiment": "POSITIVE",
                    #     "score": 0.8,
                    # })                    
                except Exception as e:
                    print(f"⚠️  Error processing text {i+j+1}: {str(e)}")
                    # Fallback result
                    results.append({
                        "text": text,
                        "sentiment": "NEUTRAL",
                        "score": 0.5,
                        "error": str(e)
                    })
            
            # Small delay between batches to respect OpenAI rate limits
            if i + batch_size < len(texts):
                time.sleep(0.5)

        
        result = {
            "sentiments_results": results,
            "data_type": "sentiment_data",
            "next_blocks": [BlockType.FILE_WRITER.value],
        }
        
        print(f"✅ Sentiment Analysis Completed!")
        return {"success": True, "result": result}
        
    except Exception as e:
        error_msg = f"Error in sentiment analysis: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def _process_toxicity_detection(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process Toxicity Detection tasks using OpenAI API"""
    print(f"Processing Toxicity Detection for block_run_id: {block_run_id}")
    
    # Get texts from config (should be populated by orchestrator)
    texts = config.get("texts", [])
    print(f"*********Toxicity Detection: {texts}")
    
    if not texts:
        error_msg = "No texts provided for toxicity detection"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    
    print(f" Detecting toxicity for {len(texts)} texts using OpenAI")
    
    try:
        results = []
        batch_size = 10  # Process in batches to avoid rate limits
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            print(f" Processing batch {i//batch_size + 1}: texts {i+1}-{min(i+batch_size, len(texts))}")
            
            for j, text in enumerate(batch_texts):
                try:
                    # Use the separate function
                    result = detect_toxicity_with_openai(text)
                    results.append(result)
                    # results.append({
                    #     "text": text,
                    #     "toxicity": "NON_TOXIC",
                    #     "score": 0.8,
                    # })
                    
                except Exception as e:
                    print(f"⚠️  Error processing text {i+j+1}: {str(e)}")
                    # Fallback result
                    results.append({
                        "text": text,
                        "toxicity": "NON_TOXIC",
                        "score": 0.1,
                        "error": str(e)
                    })
            
            # Small delay between batches to respect rate limits
            if i + batch_size < len(texts):
                time.sleep(0.5)
        
        # Calculate summary statistics
        
        result = {
            "toxicity_results": results,
            "data_type": "toxicity_data",
            "next_blocks": [BlockType.FILE_WRITER.value],
        }
        
        print(f"✅ Toxicity Detection Completed!")
        return {"success": True, "result": result}
        
    except Exception as e:
        error_msg = f"Error in toxicity detection: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def _process_file_writer(block_run_id: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process File Writer tasks - Create real CSV files with analysis results"""
    print(f"Processing File Writer for block_run_id: {block_run_id}")
    print(f"*********File Writer: config***********: {config}")
    try:
        # Get input data from config (should be populated by orchestrator)
        input_data = config.get("input_data", [])
        output_format = config.get("output_format", "csv")
        
        if not input_data:
            print(f"⚠️  No input data found for file writer")
            print(f"🔍 Available config keys: {list(config.keys())}")
            return {"success": False, "error": "No input data provided"}
        
        print(f" Writing {len(input_data)} records to CSV file")
        
        # Create output directory
        output_dir = "/app/outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        # Determine file type based on config
        block_purpose = config.get("purpose", "output")
        file_prefix = config.get("file_prefix", "results")
        
        # Generate filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        if "sentiment" in block_purpose:
            filename = f"sentiment_results_{block_run_id}_{timestamp}.csv"
        elif "toxicity" in block_purpose:
            filename = f"toxicity_results_{block_run_id}_{timestamp}.csv"
        else:
            filename = f"{file_prefix}_{block_run_id}_{timestamp}.csv"
        
        output_path = os.path.join(output_dir, filename)
        print(f"Output path: {output_path}")

        # Create CSV data
        csv_rows = []
        for index, item in enumerate(input_data, start=1):  # start=1 makes IDs start from 1
            # Handle different data structures
            if "toxicity" in item:
                # Toxicity detection results
                row = {
                    "id": index,  # Add ID field
                    "text": item.get("text", ""),
                    "toxicity_label": item.get("toxicity", ""),
                }
                # Only add error field if there's an actual error
                if item.get("error"):
                    row["error"] = item.get("error")
                csv_rows.append(row)
                
            elif "sentiment" in item:
                # Sentiment analysis results
                row = {
                    "id": index,  # Add ID field
                    "text": item.get("text", ""),
                    "sentiment_label": item.get("sentiment", ""),
                }
                # Only add error field if there's an actual error
                if item.get("error"):
                    row["error"] = item.get("error")
                csv_rows.append(row)
                
            else:
                # Generic data - filter out empty error fields and add ID
                filtered_item = {k: v for k, v in item.items() if not (k == "error" and not v)}
                filtered_item["id"] = index  # Add ID field at the beginning
                csv_rows.append(filtered_item)
        
        # Write to CSV file
        df = pd.DataFrame(csv_rows)
        df.to_csv(output_path, index=False)
        
        # Get file size
        file_size = os.path.getsize(output_path)
        file_size_kb = round(file_size / 1024, 2)
        
        result = {
            "file_info": {
                "output_path": output_path,
                "filename": filename,
                "file_size": f"{file_size_kb} KB",
                "records_written": len(csv_rows),
                "format": "csv",
                "status": "completed",
                "download_url": f"/api/v1/pipelines/download/{filename}"  # Add download URL
            },
            "data_type": "file_output",
            "next_blocks": [],  # End of pipeline
            "input_data": input_data,
            "csv_data": csv_rows  # Include the actual CSV data
        }
        
        print(f"✅ CSV file created successfully: {output_path}")
        print(f" Records written: {len(csv_rows)}")
        print(f"📁 File size: {file_size_kb} KB")
        
        return {"success": True, "result": result}
        
    except Exception as e:
        error_msg = f"Error in file writer: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}