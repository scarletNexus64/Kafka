import unittest
from decimal import Decimal
from datetime import datetime
import base64

from src.schemas.type_mapper import TypeMapper

class TestTypeMapper(unittest.TestCase):
    def setUp(self):
        self.mapper = TypeMapper()
    
    def test_get_postgres_type_for_known_fields(self):
        self.assertEqual(self.mapper.get_postgres_type('id'), 'BIGINT')
        self.assertEqual(self.mapper.get_postgres_type('email'), 'VARCHAR(255)')
        self.assertEqual(self.mapper.get_postgres_type('description'), 'TEXT')
        self.assertEqual(self.mapper.get_postgres_type('price'), 'DECIMAL(19,4)')
        self.assertEqual(self.mapper.get_postgres_type('created_at'), 'TIMESTAMP')
    
    def test_get_postgres_type_from_debezium(self):
        self.assertEqual(
            self.mapper.get_postgres_type('field', None, 'int32'),
            'INTEGER'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', None, 'io.debezium.time.MicroTimestamp'),
            'TIMESTAMP'
        )
    
    def test_infer_type_from_value(self):
        self.assertEqual(
            self.mapper.get_postgres_type('field', True),
            'BOOLEAN'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', 123),
            'INTEGER'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', 2147483648),
            'BIGINT'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', 3.14),
            'DOUBLE PRECISION'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', "short"),
            'VARCHAR(255)'
        )
        self.assertEqual(
            self.mapper.get_postgres_type('field', "x" * 300),
            'TEXT'
        )
    
    def test_process_decimal_field(self):
        value = base64.b64encode(b'\x00\x00\x04\xd2').decode('utf-8')
        
        processed = self.mapper.process_values({'price': value})
        
        self.assertIsInstance(processed['price'], Decimal)
        self.assertEqual(processed['price'], Decimal('12.34'))
    
    def test_process_timestamp_field(self):
        microseconds = 1609459200000000
        
        processed = self.mapper.process_values({'created_at': microseconds})
        
        self.assertIsInstance(processed['created_at'], datetime)
        self.assertEqual(processed['created_at'].year, 2021)
    
    def test_process_deleted_flag(self):
        processed = self.mapper.process_values({'__deleted': 'true'})
        self.assertTrue(processed['__deleted'])
        
        processed = self.mapper.process_values({'__deleted': 'false'})
        self.assertFalse(processed['__deleted'])
    
    def test_process_json_fields(self):
        data = {
            'dict_field': {'key': 'value'},
            'list_field': [1, 2, 3]
        }
        
        processed = self.mapper.process_values(data)
        
        self.assertEqual(processed['dict_field'], '{"key": "value"}')
        self.assertEqual(processed['list_field'], '[1, 2, 3]')
    
    def test_handle_none_values(self):
        processed = self.mapper.process_values({
            'field1': None,
            'field2': 'value'
        })
        
        self.assertIsNone(processed['field1'])
        self.assertEqual(processed['field2'], 'value')

if __name__ == '__main__':
    unittest.main()