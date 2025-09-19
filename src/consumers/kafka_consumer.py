import json
import logging
from typing import Dict, Any, Iterator, Optional, Tuple, List
from kafka import KafkaConsumer, ConsumerRebalanceListener
from kafka.errors import KafkaError
import time

from ..core import BaseComponent, ConsumerException
from ..monitoring.metrics import MetricsCollector

class CDCConsumerRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, logger):
        self.logger = logger
    
    def on_partitions_revoked(self, revoked):
        self.logger.info(f"Partitions revoked: {revoked}")
    
    def on_partitions_assigned(self, assigned):
        self.logger.info(f"Partitions assigned: {assigned}")

class CDCKafkaConsumer(BaseComponent):
    def __init__(self, config: Dict[str, Any], metrics: Optional[MetricsCollector] = None):
        super().__init__(config)
        self.metrics_collector = metrics
        self.consumer = None
        self._batch_size = config.get('batch_size', 100)
        self._batch_timeout = config.get('batch_timeout_ms', 1000)
        self._retry_config = config.get('retry', {
            'max_retries': 3,
            'backoff_ms': 1000,
            'max_backoff_ms': 30000
        })
        
    def _create_consumer(self) -> KafkaConsumer:
        def safe_deserializer(data):
            if data is None:
                return None
            if isinstance(data, (dict, list)):
                return data
            if not isinstance(data, (bytes, bytearray)):
                self.logger.warning(f"Unexpected data type: {type(data)}")
                return data
            try:
                return json.loads(data.decode('utf-8'))
            except Exception as e:
                self.logger.error(f"Deserialization error: {e}")
                if self.metrics_collector:
                    self.metrics_collector.increment('consumer.deserialization_errors')
                return None
        
        consumer_config = {
            'bootstrap_servers': self.config['bootstrap_servers'],
            'group_id': self.config.get('group_id', 'cdc_consumer'),
            'value_deserializer': safe_deserializer,
            'key_deserializer': safe_deserializer,
            'auto_offset_reset': self.config.get('auto_offset_reset', 'earliest'),
            'enable_auto_commit': self.config.get('enable_auto_commit', False),
            'max_poll_records': self._batch_size,
            'session_timeout_ms': self.config.get('session_timeout_ms', 30000),
            'heartbeat_interval_ms': self.config.get('heartbeat_interval_ms', 10000),
            'max_poll_interval_ms': self.config.get('max_poll_interval_ms', 300000),
        }
        
        return KafkaConsumer(*self.config['topics'], **consumer_config)
    
    def start(self) -> None:
        try:
            self.consumer = self._create_consumer()
            self._running = True
            self.logger.info(f"Kafka consumer started for topics: {self.config['topics']}")
        except Exception as e:
            raise ConsumerException(f"Failed to start consumer: {e}", error_code="CONSUMER_START_ERROR")
    
    def stop(self) -> None:
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer closed")
            except Exception as e:
                self.logger.error(f"Error closing consumer: {e}")
    
    def consume_batch(self) -> List[Tuple[str, Any, Any]]:
        if not self.consumer:
            raise ConsumerException("Consumer not initialized", error_code="CONSUMER_NOT_INITIALIZED")
        
        messages = []
        try:
            records = self.consumer.poll(timeout_ms=self._batch_timeout)
            
            for topic_partition, partition_records in records.items():
                for record in partition_records:
                    message = self._process_message(record)
                    if message:
                        messages.append(message)
                        if self.metrics_collector:
                            self.metrics_collector.increment('consumer.messages_received')
                            self.metrics_collector.increment(f'consumer.messages_received.{record.topic}')
            
            return messages
            
        except KafkaError as e:
            self.logger.error(f"Kafka error: {e}")
            if self.metrics_collector:
                self.metrics_collector.increment('consumer.kafka_errors')
            raise ConsumerException(f"Kafka consumption error: {e}", error_code="KAFKA_ERROR")
    
    def _process_message(self, record) -> Optional[Tuple[str, Any, Any]]:
        try:
            topic = record.topic
            key = record.key
            value = record.value
            
            if key and isinstance(key, dict):
                key = key.get('payload', key)
            
            if value and isinstance(value, dict):
                value = value.get('payload', value)
            
            self.logger.debug(f"Processed message from {topic}: key={key}")
            return (topic, key, value)
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            if self.metrics_collector:
                self.metrics_collector.increment('consumer.processing_errors')
            return None
    
    def commit(self) -> None:
        if self.consumer and not self.config.get('enable_auto_commit', False):
            try:
                self.consumer.commit()
                self.logger.debug("Offsets committed")
                if self.metrics_collector:
                    self.metrics_collector.increment('consumer.commits')
            except Exception as e:
                self.logger.error(f"Failed to commit offsets: {e}")
                if self.metrics_collector:
                    self.metrics_collector.increment('consumer.commit_errors')
                raise ConsumerException(f"Commit failed: {e}", error_code="COMMIT_ERROR")
    
    def health_check(self) -> Dict[str, Any]:
        health = {
            'status': 'healthy' if self._running else 'unhealthy',
            'running': self._running,
            'topics': self.config.get('topics', []),
            'group_id': self.config.get('group_id'),
        }
        
        if self.consumer:
            try:
                assignment = self.consumer.assignment()
                health['partitions'] = len(assignment)
                health['lag'] = self._calculate_lag()
            except Exception as e:
                health['error'] = str(e)
                health['status'] = 'degraded'
        
        return health
    
    def _calculate_lag(self) -> int:
        total_lag = 0
        try:
            for partition in self.consumer.assignment():
                committed = self.consumer.committed(partition)
                if committed:
                    position = self.consumer.position(partition)
                    total_lag += position - committed
        except Exception as e:
            self.logger.debug(f"Could not calculate lag: {e}")
        return total_lag