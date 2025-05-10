#!/usr/bin/env python3
"""
Kafka to HDFS Data Ingestor with User-Based Partitioning
- Uses full SHA-256 hash for directory structure
- Includes precise timestamp in filename
- Maintains batch processing and rotation
"""

from confluent_kafka import Consumer, KafkaException
from hdfs import InsecureClient
import json
import time
import logging
from datetime import datetime
import socket
import hashlib
import os

# Configuration
CONFIG = {
    'kafka': {
        'bootstrap.servers': 'localhost:9093',
        'group.id': 'python-ingestor',
        'security.protocol': 'PLAINTEXT',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    },
    'hdfs': {
        'url': 'http://localhost:9870',
        'user': 'hdfs',
        'root_path': '/data/user_events',
        'hash_depth': 3,  # Use first 3 hex chars for directory levels
        'roll_interval': 3600  # Rotate files every hour
    },
    'topics': ['user-event-topic'],
    'batch_size': 1000,
    'batch_timeout': 30  # seconds
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaToHDFSIngestor:
    def __init__(self, config):
        self.config = config
        self.hdfs_client = None
        self.kafka_consumer = None
        self.current_files = {}  # Track current file per user path
        self.last_roll_time = time.time()
        self.message_buffer = []

    def connect_kafka(self):
        """Connect to Kafka cluster"""
        try:
            self.kafka_consumer = Consumer(self.config['kafka'])
            self.kafka_consumer.subscribe(self.config['topics'])
            logger.info(f"Subscribed to Kafka topics: {self.config['topics']}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {str(e)}")
            raise
    def connect_hdfs(self):
        """Connect to HDFS with proper URL handling"""
        try:
            # Validate and clean the HDFS URL
            hdfs_url = self.config['hdfs']['url']
            if not hdfs_url.startswith(('http://', 'https://')):
                hdfs_url = f"http://{hdfs_url}"
            
            self.hdfs_client = InsecureClient(
                hdfs_url,
                user=self.config['hdfs']['user'],
                timeout=30,
                root=self.config['hdfs']['root_path']
            )
            
            # Test connection
            self.hdfs_client.status('/')
            logger.info(f"Connected to HDFS at {hdfs_url}")
        except Exception as e:
            logger.error(f"HDFS connection failed to {self.config['hdfs']['url']}: {str(e)}")
            raise RuntimeError(f"HDFS connection failed: {str(e)}")

    def get_user_hash_path(self, user_id):
        """Generate proper HDFS path with forward slashes"""
        user_str = str(user_id) if user_id else "unknown"
        user_str = user_str.replace('/', '_')  # Sanitize user ID
        
        hash_hex = hashlib.sha256(user_str.encode('utf-8')).hexdigest()
        hash_parts = [f"hash_{hash_hex[i*2:(i+1)*2]}" 
                     for i in range(self.config['hdfs']['hash_depth'])]
        
        # Build proper HDFS path with forward slashes
        path_parts = [self.config['hdfs']['root_path']] + hash_parts + [f"user_{user_str}"]
        return '/'.join(path_parts)

    def get_hdfs_path(self, user_id):
        """Generate complete HDFS path with proper encoding"""
        dir_path = self.get_user_hash_path(user_id)
        now = datetime.now()
        filename = f"{now.strftime('%Y-%m-%d-%H-%M-%S-%f')}-event.json"
        return f"{dir_path}/{filename}"

    def write_batch(self):
        """Write buffered messages to HDFS with robust error handling"""
        if not self.message_buffer:
            return

        try:
            current_time = time.time()
            messages_by_user = {}
            
            # Group messages by user
            for msg in self.message_buffer:
                try:
                    print(msg)
                    user_id = msg['value'].get('userId', 'userId') if isinstance(msg['value'], dict) else 'unknown'
                    messages_by_user.setdefault(user_id, []).append(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue

            # Process each user's messages
            for user_id, messages in messages_by_user.items():
                try:
                    file_path = self.current_files.get(user_id)
                    
                    # Rotate file if needed
                    if (file_path is None or 
                        current_time - self.last_roll_time > self.config['hdfs']['roll_interval']):
                        
                        file_path = self.get_hdfs_path(user_id)
                        self.current_files[user_id] = file_path
                        self.last_roll_time = current_time
                        
                        # Create parent directory
                        dir_path = file_path[:file_path.rindex('/')]
                        self.hdfs_client.makedirs(dir_path)
                        
                        # Create empty file
                        self.hdfs_client.write(file_path, data='', overwrite=False)
                        logger.info(f"Created new file: {file_path}")

                    # Write messages
                    data = '\n'.join(json.dumps(msg) for msg in messages) + '\n'
                    with self.hdfs_client.write(
                        file_path,
                        overwrite=False,
                        append=True,
                        encoding='utf-8'
                    ) as writer:
                        writer.write(data)
                    
                    logger.info(f"Wrote {len(messages)} messages for user {user_id}")
                
                except Exception as e:
                    logger.error(f"Failed to write messages for user {user_id}: {str(e)}")
                    continue

            self.message_buffer = []

        except Exception as e:
            logger.error(f"Batch write failed: {str(e)}")
            raise


    def process_message(self, msg):
        """Process individual Kafka message"""
        try:
            value = msg.value().decode('utf-8')
            message_data = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                'value': json.loads(value) if value else None
            }
            self.message_buffer.append(message_data)
            logger.debug("New message processed")
        except json.JSONDecodeError:
            self.message_buffer.append({
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'value': msg.value().decode('utf-8', errors='replace')
            })
        except Exception as e:
            logger.error(f"Message processing error: {str(e)}")

    def run(self):
        """Main processing loop"""
        try:
            self.connect_hdfs()
            self.connect_kafka()

            batch_start = time.time()
            while True:
                msg = self.kafka_consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                self.process_message(msg)

                if (len(self.message_buffer) >= self.config['batch_size'] or
                    time.time() - batch_start >= self.config['batch_timeout']):
                    self.write_batch()
                    batch_start = time.time()

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            if self.message_buffer:
                self.write_batch()

            if self.kafka_consumer:
                self.kafka_consumer.close()
            logger.info("Ingestor stopped")

if __name__ == "__main__":
    ingestor = KafkaToHDFSIngestor(CONFIG)
    ingestor.run()

