import psycopg2
from psycopg2 import pool, extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from typing import Dict, Any, List, Optional, Tuple
from contextlib import contextmanager
import time

from ..core import BaseComponent, WriterException, RetryableException
from ..schemas.type_mapper import TypeMapper
from ..monitoring.metrics import MetricsCollector

class PostgresWriter(BaseComponent):
    def __init__(self, config: Dict[str, Any], metrics: Optional[MetricsCollector] = None):
        super().__init__(config)
        self.metrics_collector = metrics
        self.connection_pool = None
        self.type_mapper = TypeMapper()
        self._retry_config = config.get('retry', {
            'max_retries': 3,
            'backoff_ms': 1000,
            'max_backoff_ms': 30000
        })
        self._batch_config = config.get('batch', {
            'size': 100,
            'timeout_ms': 1000
        })
        self._created_tables = set()
        
    def start(self) -> None:
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.get('min_connections', 1),
                maxconn=self.config.get('max_connections', 10),
                host=self.config['host'],
                port=self.config.get('port', 5432),
                database=self.config.get('database') or self.config.get('dbname'),
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=self.config.get('connect_timeout', 10)
            )
            self._running = True
            self.logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            raise WriterException(f"Failed to initialize connection pool: {e}", error_code="POOL_INIT_ERROR")
    
    def stop(self) -> None:
        self._running = False
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                self.logger.info("PostgreSQL connection pool closed")
            except Exception as e:
                self.logger.error(f"Error closing connection pool: {e}")
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def write_batch(self, records: List[Tuple[str, Dict, Dict]]) -> Dict[str, int]:
        if not records:
            return {'processed': 0, 'errors': 0}
        
        stats = {'processed': 0, 'errors': 0, 'inserts': 0, 'updates': 0, 'deletes': 0}
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    for table, key_data, value_data in records:
                        try:
                            operation = self._process_record(cursor, table, key_data, value_data)
                            stats['processed'] += 1
                            stats[operation] += 1
                            
                            if self.metrics_collector:
                                self.metrics_collector.increment(f'writer.{operation}')
                                self.metrics_collector.increment(f'writer.{table}.{operation}')
                        
                        except Exception as e:
                            self.logger.error(f"Error processing record for {table}: {e}")
                            stats['errors'] += 1
                            if self.metrics_collector:
                                self.metrics_collector.increment('writer.errors')
                            
                            if not self.config.get('continue_on_error', True):
                                conn.rollback()
                                raise
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    raise WriterException(f"Batch write failed: {e}", error_code="BATCH_WRITE_ERROR")
        
        return stats
    
    def _process_record(self, cursor, table: str, key_data: Dict, value_data: Optional[Dict]) -> str:
        if table not in self._created_tables:
            self._ensure_table_exists(cursor, table, key_data, value_data)
            self._created_tables.add(table)
        
        pk_field = self._extract_pk_field(key_data)
        if not pk_field:
            raise WriterException(f"No primary key found for table {table}", error_code="NO_PRIMARY_KEY")
        
        if value_data is None or (isinstance(value_data, dict) and value_data.get('__deleted') == 'true'):
            return self._delete_record(cursor, table, pk_field, key_data)
        else:
            return self._upsert_record(cursor, table, pk_field, value_data)
    
    def _upsert_record(self, cursor, table: str, pk_field: str, data: Dict) -> str:
        processed_data = self.type_mapper.process_values(data)
        
        columns = list(processed_data.keys())
        values = list(processed_data.values())
        
        placeholders = ', '.join(['%s'] * len(values))
        column_list = ', '.join(columns)
        update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != pk_field])
        
        sql = f"""
            INSERT INTO {table} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({pk_field}) DO UPDATE SET {update_set}
            RETURNING xmax::text::int > 0 as was_updated
        """
        
        cursor.execute(sql, values)
        was_updated = cursor.fetchone()[0] if cursor.rowcount > 0 else False
        
        return 'updates' if was_updated else 'inserts'
    
    def _delete_record(self, cursor, table: str, pk_field: str, key_data: Dict) -> str:
        pk_value = key_data.get(pk_field)
        if not pk_value:
            raise WriterException(f"Primary key value missing for delete operation", error_code="PK_VALUE_MISSING")
        
        sql = f"DELETE FROM {table} WHERE {pk_field} = %s"
        cursor.execute(sql, (pk_value,))
        
        return 'deletes'
    
    def _ensure_table_exists(self, cursor, table: str, key_data: Dict, value_data: Optional[Dict]):
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (table,))
        
        if not cursor.fetchone()[0]:
            self._create_table(cursor, table, key_data, value_data)
    
    def _create_table(self, cursor, table: str, key_data: Dict, value_data: Optional[Dict]):
        columns = []
        
        pk_field = self._extract_pk_field(key_data)
        if pk_field:
            columns.append(f"{pk_field} BIGINT PRIMARY KEY")
        
        if value_data and isinstance(value_data, dict):
            for field_name, field_value in value_data.items():
                if field_name != pk_field:
                    col_type = self.type_mapper.get_postgres_type(field_name, field_value)
                    columns.append(f"{field_name} {col_type}")
        
        if columns:
            columns_sql = ', '.join(columns)
            sql = f"CREATE TABLE IF NOT EXISTS public.{table} ({columns_sql})"
            cursor.execute(sql)
            self.logger.info(f"Created table {table}")
    
    def _extract_pk_field(self, key_data: Dict) -> Optional[str]:
        if not key_data or not isinstance(key_data, dict):
            return None
        
        if 'payload' in key_data and isinstance(key_data['payload'], dict):
            keys = list(key_data['payload'].keys())
            return keys[0] if keys else None
        
        keys = list(key_data.keys())
        return keys[0] if keys else None
    
    def health_check(self) -> Dict[str, Any]:
        health = {
            'status': 'healthy' if self._running else 'unhealthy',
            'running': self._running,
            'database': self.config.get('database'),
            'host': self.config.get('host'),
        }
        
        if self.connection_pool:
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        health['connection'] = 'ok'
                        health['pool_size'] = self.connection_pool.maxconn
            except Exception as e:
                health['error'] = str(e)
                health['status'] = 'unhealthy'
                health['connection'] = 'failed'
        
        return health