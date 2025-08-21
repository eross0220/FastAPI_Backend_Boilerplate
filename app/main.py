import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import api_router
from app.core import settings
from app.core.kafka_client import KafkaClient
import json
import asyncio
import threading
from typing import List
from contextlib import asynccontextmanager
import time
from datetime import datetime

# Store active WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.kafka_client = KafkaClient()
        self.consumer_thread = None
        self.stop_consumer = threading.Event()
        self.main_loop = None  # Store the main event loop

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"WebSocket connected. Total connections: {len(self.active_connections)}")
        
        # Store the main event loop when first connecting
        if self.main_loop is None:
            self.main_loop = asyncio.get_event_loop()
            print(f"📡 Stored main event loop: {self.main_loop}")
        
        # Start Kafka consumer if this is the first connection
        if len(self.active_connections) == 1:
            self._start_kafka_consumer()

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        print(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        if not self.active_connections:
            return
            
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                print(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)
        
        # Remove disconnected connections
        for conn in disconnected:
            self.active_connections.remove(conn)

    def _start_kafka_consumer(self):
        """Start Kafka consumer in a separate thread"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            return
            
        if self.main_loop is None:
            print("❌ No main event loop available, cannot start consumer")
            return
            
        print("🚀 Starting Kafka consumer thread...")
        self.stop_consumer.clear()
        self.consumer_thread = threading.Thread(target=self._consume_kafka_events_thread, daemon=True)
        self.consumer_thread.start()
        print("✅ Started Kafka consumer thread")

    def _stop_kafka_consumer(self):
        """Stop Kafka consumer thread"""
        if self.consumer_thread:
            print("Stopping Kafka consumer...")
            self.stop_consumer.set()
            self.consumer_thread.join(timeout=5)  # Increased timeout
            self.consumer_thread = None
            print("Stopped Kafka consumer thread")

    def _consume_kafka_events_thread(self):
        """Kafka consumer running in separate thread"""
        consumer = None
        try:
            print("🔄 Creating Kafka consumer...")
            consumer = self.kafka_client.get_consumer()
            if not consumer:
                print("❌ Failed to create Kafka consumer")
                return
                
            print("📡 Subscribing to topics...")
            consumer.subscribe(['block_events', 'pipeline_events'])
            print("🚀 Started consuming Kafka events for WebSocket broadcast")
            
            while not self.stop_consumer.is_set():
                try:
                    # Check if consumer is still valid
                    if consumer is None or consumer._closed:
                        print(" Consumer is closed, recreating...")
                        consumer = self.kafka_client.get_consumer()
                        if not consumer:
                            print("❌ Failed to recreate consumer, retrying in 5 seconds...")
                            time.sleep(5)
                            continue
                        consumer.subscribe(['block_events', 'pipeline_events'])
                        print("✅ Recreated and resubscribed consumer")
                    
                    # Poll for messages with timeout
                    messages = consumer.poll(timeout_ms=1000)
                    
                    if not messages:
                        continue
                    
                    # Process each topic partition and its messages
                    for topic_partition, partition_messages in messages.items():
                        topic = topic_partition.topic                        
                        # Process each message in the partition
                        for message in partition_messages:
                            try:
                                topic_name = message.topic
                                event_data = message.value
                                offset = message.offset
                                timestamp = message.timestamp
                                if event_data is None:
                                    continue
                                
                                # Print beautiful, simple message
                                self._print_beautiful_event(event_data, topic_name, offset, timestamp)
                                
                                # Format the event for WebSocket broadcast
                                formatted_event = {
                                    'timestamp': event_data.get('timestamp', ''),
                                    'event_type': event_data.get('event_type', ''),
                                    'topic': topic_name,
                                    'pipeline_run_id': event_data.get('pipeline_run_id'),
                                    'pipeline_id': event_data.get('pipeline_id'),
                                    'block_run_id': event_data.get('block_run_id'),
                                    'success': event_data.get('success'),
                                    'message': self._format_event_message(event_data),
                                    'level': self._get_event_level(event_data),
                                    'raw_data': event_data
                                }
                                
                                # Use the stored main event loop for broadcasting
                                if self.active_connections and self.main_loop:
                                    try:
                                        if self.main_loop.is_running():
                                            asyncio.run_coroutine_threadsafe(
                                                self.broadcast(json.dumps(formatted_event)), 
                                                self.main_loop  # Use stored main loop
                                            )
                                            # print(f"✅ Broadcasted to WebSocket: {event_data.get('event_type')}")
                                        else:
                                            print("⚠️ Main event loop not running")
                                    except Exception as e:
                                        print(f"❌ Error broadcasting message: {e}")
                                else:
                                    if not self.active_connections:
                                        print("⚠️ No active WebSocket connections")
                                    if not self.main_loop:
                                        print("⚠️ No main event loop available")
                                
                            except Exception as e:
                                print(f"❌ Error processing individual message: {e}")
                                print(f"   Message type: {type(message)}")
                                print(f"   Message content: {message}")
                                continue
                    
                except Exception as e:
                    print(f"❌ Error in main consumer loop: {e}")
                    # If consumer is closed, try to recreate it
                    if "closed" in str(e).lower() or "KafkaConsumer is closed" in str(e):
                        print("🔄 Consumer closed, will recreate on next iteration...")
                        consumer = None
                    continue
                    
        except Exception as e:
            print(f"❌ Fatal error in Kafka consumer thread: {e}")
        finally:
            print("🛑 Kafka consumer thread cleanup starting...")
            if consumer and not consumer._closed:
                try:
                    consumer.close()
                    print("✅ Kafka consumer closed successfully")
                except Exception as e:
                    print(f"❌ Error closing consumer: {e}")
            print("🛑 Kafka consumer thread stopped")

    def _format_event_message(self, event_data: dict) -> str:
        """Format event data into a readable message"""
        event_type = event_data.get('event_type', '')
        
        if event_type == 'pipeline_started':
            return f"Pipeline {event_data.get('pipeline_run_id')} started"
        elif event_type == 'pipeline_completed':
            success = event_data.get('success', False)
            return f"Pipeline {event_data.get('pipeline_run_id')} {'completed successfully' if success else 'failed'}"
        elif event_type == 'block_started':
            return f"Block {event_data.get('block_run_id')} ({event_data.get('block_type', 'unknown')}) started"
        elif event_type == 'block_completed':
            success = event_data.get('success', False)
            return f"Block {event_data.get('block_run_id')} {'completed successfully' if success else 'failed'}"
        elif event_type == 'block_failed':
            return f"Block {event_data.get('block_run_id')} failed"
        else:
            return f"Event: {event_type}"

    def _get_event_level(self, event_data: dict) -> str:
        """Determine log level based on event type and success"""
        event_type = event_data.get('event_type', '')
        success = event_data.get('success', True)
        
        if 'failed' in event_type or success is False:
            return 'ERROR'
        elif 'started' in event_type:
            return 'INFO'
        elif 'completed' in event_type:
            return 'INFO'
        else:
            return 'INFO'

    def _print_beautiful_event(self, event_data: dict, topic: str, offset: int, timestamp: int):
        """Print beautiful, simple messages for Kafka events"""
        event_type = event_data.get('event_type', '')
        event_timestamp = event_data.get('timestamp', '')
        
        # Extract relevant IDs
        pipeline_run_id = event_data.get('pipeline_run_id')
        pipeline_id = event_data.get('pipeline_id')
        block_run_id = event_data.get('block_run_id')
        block_type = event_data.get('block_type', '')
        success = event_data.get('success')
        
        # Format Kafka timestamp (milliseconds since epoch)
        try:
            kafka_time = datetime.fromtimestamp(timestamp / 1000)
            kafka_time_str = kafka_time.strftime('%H:%M:%S')
        except:
            kafka_time_str = "N/A"
        
        # Format event timestamp if available
        try:
            if event_timestamp:
                dt = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
                event_time_str = dt.strftime('%H:%M:%S.%f')[:-3]  # Include milliseconds
            else:
                event_time_str = "N/A"
        except:
            event_time_str = "N/A"
        
        # Print based on event type with detailed information
        if event_type == 'pipeline_started':
            print(f"🚀 [{kafka_time_str}] Pipeline #{pipeline_run_id} (ID: {pipeline_id}) STARTED")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        elif event_type == 'pipeline_completed':
            status = "✅ COMPLETED" if success else "❌ FAILED"
            print(f"🏁 [{kafka_time_str}] Pipeline #{pipeline_run_id} {status}")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        elif event_type == 'block_started':
            print(f"⚡ [{kafka_time_str}] Block #{block_run_id} ({block_type}) STARTED")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        elif event_type == 'block_completed':
            status = "✅ COMPLETED" if success else "❌ FAILED"
            print(f" [{kafka_time_str}] Block #{block_run_id} ({block_type}) {status}")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        elif event_type == 'block_failed':
            print(f" [{kafka_time_str}] Block #{block_run_id} ({block_type}) FAILED")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        else:
            print(f"📝 [{kafka_time_str}] {topic}: {event_type}")
            print(f"   📍 Topic: {topic} | Offset: {offset} | Event Time: {event_time_str}")
        
        print()  # Empty line for better readability

manager = ConnectionManager()

# Remove the async Kafka consumer function since we're using threads now
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting up FastAPI application...")
    # No need to start Kafka consumer here - it starts when first WebSocket connects
    yield
    # Shutdown
    print("Shutting down FastAPI application...")
    manager._stop_kafka_consumer()

app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan
)

# Add CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3002"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define WebSocket endpoint BEFORE including the API router
@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial connection message
        await websocket.send_text(json.dumps({
            'type': 'connection',
            'message': 'Connected to log stream',
            'timestamp': asyncio.get_event_loop().time()
        }))
        
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for any message from client (ping/pong)
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
            except Exception as e:
                print(f"WebSocket error: {e}")
                break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)

# Include API router AFTER WebSocket endpoint
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "websocket_connections": len(manager.active_connections)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
