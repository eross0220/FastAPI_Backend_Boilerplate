import pandas as pd
import os
from typing import Dict, Any
from pathlib import Path

def process_task(block_run_id: int, block_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Universal worker that can handle any task type"""
    try:
        print(f"Processing {block_type} for block_run_id: {block_run_id}")
        
        if block_type == "csv_reader":
            return _process_csv_reader(config)
        elif block_type == "sentiment_analysis":
            return _process_sentiment_analysis(config)
        elif block_type == "toxicity_detection":
            return _process_toxicity_detection(config)
        elif block_type == "file_writer":
            return _process_file_writer(config)
        else:
            raise ValueError(f"Unknown block type: {block_type}")
            
    except Exception as e:
        print(f"Task failed for block_run_id: {block_run_id}: {e}")
        return {"success": False, "error": str(e), "block_run_id": block_run_id}

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
    print(f"@@@@@ Processing File Writer for block_run_id: {config.get('block_run_id')}")
    
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
    """Process File Writer tasks"""
    data = config.get("data", [])
    output_path = config.get("output_path", f"./data/output_{config.get('block_run_id', 'unknown')}.csv")
    columns = config.get("columns", [])
    
    # Ensure output directory exists
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write to CSV
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    
    result = {
        "file_path": output_path,
        "row_count": len(data),
        "columns": columns
    }
    
    print(f"File Writer completed, wrote {len(data)} rows to {output_path}")
    return {"success": True, "result": result}