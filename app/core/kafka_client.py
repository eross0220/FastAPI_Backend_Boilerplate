from kafka import KafkaProducer, KafkaConsumer
import json
import os
from typing import Dict, Any

class KafkaClient:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer = None
    
    def get_producer(self):
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
        return self.producer
    
    def publish_event(self, topic: str, event_data: Dict[str, Any], key: str = None):
        """Publish event to Kafka topic"""
        try:
            producer = self.get_producer()
            producer.send(topic, key=key, value=event_data)
            producer.flush()
        except Exception as e:
            print(f"Error publishing to Kafka: {e}")