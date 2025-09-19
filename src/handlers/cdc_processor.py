import logging
from typing import Dict, Any, List, Optional, Tuple
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, Future

from ..consumers.kafka_consumer import CDCKafkaConsumer
from ..writers.postgres_writer import PostgresWriter
from ..monitoring.metrics import MetricsCollector, HealthChecker
from ..core.exceptions import CDCException

class CDCProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.metrics = MetricsCollector()
        self.health_checker = HealthChecker()
        
        self.consumer = CDCKafkaConsumer(config['kafka'], self.metrics)
        self.writer = PostgresWriter(config['postgres'], self.metrics)
        
        self.health_checker.register_component('consumer', self.consumer)
        self.health_checker.register_component('writer', self.writer)
        
        self._running = False
        self._shutdown_event = threading.Event()
        self._work_queue = queue.Queue(maxsize=config.get('processing', {}).get('queue_size', 1000))
        self._executor = ThreadPoolExecutor(max_workers=config.get('processing', {}).get('max_workers', 4))
        self._consumer_thread = None
        self._processor_threads = []
        
        self._error_handler = ErrorHandler(config.get('error_handling', {}), self.metrics)
    
    def start(self):
        try:
            self.logger.info("Starting CDC Processor...")
            
            self.consumer.start()
            self.writer.start()
            
            self._running = True
            
            self._consumer_thread = threading.Thread(target=self._consume_loop, name="consumer-thread")
            self._consumer_thread.start()
            
            num_processors = self.config.get('processing', {}).get('max_workers', 4)
            for i in range(num_processors):
                thread = threading.Thread(target=self._process_loop, name=f"processor-{i}")
                thread.start()
                self._processor_threads.append(thread)
            
            self.logger.info(f"CDC Processor started with {num_processors} workers")
            
        except Exception as e:
            self.logger.error(f"Failed to start processor: {e}")
            self.stop()
            raise
    
    def _consume_loop(self):
        while self._running:
            try:
                messages = self.consumer.consume_batch()
                
                if messages:
                    self.logger.debug(f"Consumed {len(messages)} messages")
                    self.metrics.increment('processor.messages_consumed', len(messages))
                    
                    grouped = self._group_messages_by_table(messages)
                    
                    for table, records in grouped.items():
                        try:
                            self._work_queue.put((table, records), timeout=5)
                            self.metrics.increment('processor.batches_queued')
                        except queue.Full:
                            self.logger.warning(f"Work queue full, backing off")
                            self.metrics.increment('processor.queue_full')
                            time.sleep(1)
                
                else:
                    time.sleep(0.1)
                    
            except Exception as e:
                self.logger.error(f"Error in consumer loop: {e}")
                self.metrics.increment('processor.consumer_errors')
                if not self._error_handler.handle_error(e):
                    break
    
    def _process_loop(self):
        while self._running:
            try:
                try:
                    table, records = self._work_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                with self.metrics.timer('processor.batch_processing_time'):
                    self._process_batch(table, records)
                
                self._work_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in processor loop: {e}")
                self.metrics.increment('processor.processor_errors')
    
    def _process_batch(self, table: str, records: List[Tuple[Dict, Dict]]):
        try:
            formatted_records = [(table, key, value) for key, value in records]
            
            stats = self.writer.write_batch(formatted_records)
            
            self.metrics.increment('processor.records_processed', stats['processed'])
            self.metrics.increment('processor.records_failed', stats['errors'])
            self.metrics.increment(f'processor.{table}.processed', stats['processed'])
            
            if not self.config['kafka'].get('enable_auto_commit', False):
                self.consumer.commit()
                
            self.logger.debug(f"Processed batch for {table}: {stats}")
            
        except Exception as e:
            self.logger.error(f"Failed to process batch for {table}: {e}")
            self.metrics.increment('processor.batch_errors')
            
            if not self._error_handler.handle_error(e, context={'table': table, 'record_count': len(records)}):
                raise
    
    def _group_messages_by_table(self, messages: List[Tuple[str, Any, Any]]) -> Dict[str, List[Tuple[Dict, Dict]]]:
        grouped = {}
        
        for topic, key, value in messages:
            if not topic.startswith("cdc.public."):
                self.logger.warning(f"Unexpected topic format: {topic}")
                continue
            
            table = topic.replace("cdc.public.", "")
            
            if table not in grouped:
                grouped[table] = []
            
            grouped[table].append((key, value))
        
        return grouped
    
    def stop(self):
        self.logger.info("Stopping CDC Processor...")
        self._running = False
        self._shutdown_event.set()
        
        shutdown_timeout = self.config.get('processing', {}).get('shutdown_timeout', 30)
        
        if self._consumer_thread:
            self._consumer_thread.join(timeout=shutdown_timeout)
        
        for thread in self._processor_threads:
            thread.join(timeout=shutdown_timeout)
        
        self._work_queue.join()
        
        self._executor.shutdown(wait=True, timeout=shutdown_timeout)
        
        self.consumer.stop()
        self.writer.stop()
        
        self.logger.info("CDC Processor stopped")
    
    def get_status(self) -> Dict[str, Any]:
        return {
            'running': self._running,
            'queue_size': self._work_queue.qsize(),
            'health': self.health_checker.check_health(),
            'metrics': self.metrics.get_metrics()
        }

class ErrorHandler:
    def __init__(self, config: Dict[str, Any], metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(self.__class__.__name__)
        self._error_counts = {}
        self._last_error_time = {}
        
        self.max_consecutive_errors = config.get('max_consecutive_errors', 10)
        self.error_reset_interval = config.get('error_reset_interval', 60)
        self.backoff_base = config.get('backoff_base', 2)
        self.max_backoff = config.get('max_backoff', 300)
    
    def handle_error(self, error: Exception, context: Dict[str, Any] = None) -> bool:
        error_type = type(error).__name__
        current_time = time.time()
        
        if error_type not in self._error_counts:
            self._error_counts[error_type] = 0
            self._last_error_time[error_type] = 0
        
        if current_time - self._last_error_time[error_type] > self.error_reset_interval:
            self._error_counts[error_type] = 0
        
        self._error_counts[error_type] += 1
        self._last_error_time[error_type] = current_time
        
        self.metrics.increment(f'error_handler.{error_type}')
        
        if self._error_counts[error_type] >= self.max_consecutive_errors:
            self.logger.error(f"Max consecutive errors reached for {error_type}")
            return False
        
        backoff_time = min(
            self.backoff_base ** self._error_counts[error_type],
            self.max_backoff
        )
        
        self.logger.warning(f"Error {error_type} occurred (count: {self._error_counts[error_type]}), backing off for {backoff_time}s")
        time.sleep(backoff_time)
        
        return True