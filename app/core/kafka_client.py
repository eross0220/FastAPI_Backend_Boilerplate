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
            # print(f"📡 [KAFKA] Published event to topic '{topic}'")
        except Exception as e:
            print(f"❌ Error publishing to Kafka: {e}")

    def get_consumer(self):
        """Get Kafka consumer for reading events"""
        if not hasattr(self, 'consumer') or not self.consumer:
            try:
                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    auto_offset_reset='latest',  # Start from latest messages
                    enable_auto_commit=True,
                    group_id='pipeline_logs_consumer',  # Consumer group
                    consumer_timeout_ms=1000
                )
                print(f"✅ Kafka consumer connected to {self.bootstrap_servers}")
            except Exception as e:
                print(f"❌ Failed to create Kafka consumer: {e}")
                return None
        return self.consumer