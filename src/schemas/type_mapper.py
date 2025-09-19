import base64
from decimal import Decimal
from typing import Any, Dict, Optional
import json
from datetime import datetime

class TypeMapper:
    def __init__(self):
        self.type_mappings = {
            'int32': 'INTEGER',
            'int64': 'BIGINT',
            'float32': 'REAL',
            'float64': 'DOUBLE PRECISION',
            'boolean': 'BOOLEAN',
            'string': 'VARCHAR(255)',
            'bytes': 'BYTEA',
            'io.debezium.time.Date': 'DATE',
            'io.debezium.time.Time': 'TIME',
            'io.debezium.time.MicroTime': 'TIME',
            'io.debezium.time.Timestamp': 'TIMESTAMP',
            'io.debezium.time.MicroTimestamp': 'TIMESTAMP',
            'io.debezium.time.ZonedTimestamp': 'TIMESTAMPTZ',
            'io.debezium.data.Json': 'JSONB',
            'io.debezium.data.Uuid': 'UUID',
        }
        
        self.field_overrides = {
            'id': 'BIGINT',
            'uuid': 'UUID',
            'email': 'VARCHAR(255)',
            'phone': 'VARCHAR(50)',
            'url': 'TEXT',
            'description': 'TEXT',
            'content': 'TEXT',
            'body': 'TEXT',
            'price': 'DECIMAL(19,4)',
            'amount': 'DECIMAL(19,4)',
            'total': 'DECIMAL(19,4)',
            'quantity': 'INTEGER',
            'created_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP',
            'deleted_at': 'TIMESTAMP',
            '__deleted': 'BOOLEAN',
        }
    
    def get_postgres_type(self, field_name: str, field_value: Any = None, debezium_type: str = None) -> str:
        if field_name in self.field_overrides:
            return self.field_overrides[field_name]
        
        if debezium_type and debezium_type in self.type_mappings:
            return self.type_mappings[debezium_type]
        
        if field_value is not None:
            return self._infer_type_from_value(field_value)
        
        return 'TEXT'
    
    def _infer_type_from_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return 'BOOLEAN'
        elif isinstance(value, int):
            if value > 2147483647 or value < -2147483648:
                return 'BIGINT'
            return 'INTEGER'
        elif isinstance(value, float):
            return 'DOUBLE PRECISION'
        elif isinstance(value, Decimal):
            return 'DECIMAL(19,4)'
        elif isinstance(value, datetime):
            return 'TIMESTAMP'
        elif isinstance(value, dict):
            return 'JSONB'
        elif isinstance(value, list):
            return 'JSONB'
        elif isinstance(value, bytes):
            return 'BYTEA'
        elif isinstance(value, str):
            if len(value) > 255:
                return 'TEXT'
            return 'VARCHAR(255)'
        else:
            return 'TEXT'
    
    def process_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        processed = {}
        
        for key, value in data.items():
            processed[key] = self._process_value(key, value)
        
        return processed
    
    def _process_value(self, field_name: str, value: Any) -> Any:
        if value is None:
            return None
        
        if field_name in ('price', 'amount', 'total') and isinstance(value, str):
            try:
                decoded_bytes = base64.b64decode(value)
                return Decimal(decoded_bytes.hex()) / 100
            except Exception:
                return value
        
        if field_name == '__deleted' and isinstance(value, str):
            return value.lower() == 'true'
        
        if field_name.endswith('_at') and isinstance(value, int):
            if value > 1000000000000000:
                return datetime.fromtimestamp(value / 1000000)
            elif value > 1000000000000:
                return datetime.fromtimestamp(value / 1000)
            else:
                return datetime.fromtimestamp(value)
        
        if isinstance(value, dict) or isinstance(value, list):
            return json.dumps(value)
        
        return value
    
    def get_debezium_converter(self, debezium_type: str):
        converters = {
            'io.debezium.time.Date': lambda v: datetime.fromtimestamp(v * 86400) if v else None,
            'io.debezium.time.Time': lambda v: datetime.fromtimestamp(v / 1000) if v else None,
            'io.debezium.time.MicroTime': lambda v: datetime.fromtimestamp(v / 1000000) if v else None,
            'io.debezium.time.Timestamp': lambda v: datetime.fromtimestamp(v / 1000) if v else None,
            'io.debezium.time.MicroTimestamp': lambda v: datetime.fromtimestamp(v / 1000000) if v else None,
            'io.debezium.data.Json': lambda v: json.loads(v) if isinstance(v, str) else v,
        }
        return converters.get(debezium_type, lambda v: v)