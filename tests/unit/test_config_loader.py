import unittest
import tempfile
import os
import yaml
from pathlib import Path

from src.config.config_loader import ConfigLoader
from src.core.exceptions import ConfigurationException

class TestConfigLoader(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = Path(self.temp_dir) / 'config.yaml'
        
        self.valid_config = {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topics': ['topic1', 'topic2'],
                'group_id': 'test_group'
            },
            'postgres': {
                'host': 'localhost',
                'port': 5432,
                'database': 'testdb',
                'user': 'testuser',
                'password': 'testpass'
            }
        }
        
        with open(self.config_file, 'w') as f:
            yaml.dump(self.valid_config, f)
    
    def tearDown(self):
        if self.config_file.exists():
            self.config_file.unlink()
        Path(self.temp_dir).rmdir()
    
    def test_load_valid_config(self):
        loader = ConfigLoader(str(self.config_file))
        config = loader.load()
        
        self.assertEqual(config['kafka']['bootstrap_servers'], 'localhost:9092')
        self.assertEqual(config['postgres']['host'], 'localhost')
        self.assertIn('topic1', config['kafka']['topics'])
    
    def test_apply_defaults(self):
        loader = ConfigLoader(str(self.config_file))
        config = loader.load()
        
        self.assertEqual(config['kafka']['auto_offset_reset'], 'earliest')
        self.assertEqual(config['kafka']['enable_auto_commit'], False)
        self.assertEqual(config['postgres']['port'], 5432)
        self.assertEqual(config['logging']['level'], 'INFO')
    
    def test_env_overrides(self):
        os.environ['CDC_KAFKA_GROUP_ID'] = 'env_group'
        os.environ['CDC_POSTGRES_PORT'] = '5433'
        os.environ['CDC_METRICS_ENABLED'] = 'false'
        
        loader = ConfigLoader(str(self.config_file))
        config = loader.load()
        
        self.assertEqual(config['kafka']['group_id'], 'env_group')
        self.assertEqual(config['postgres']['port'], 5433)
        self.assertFalse(config['metrics']['enabled'])
        
        del os.environ['CDC_KAFKA_GROUP_ID']
        del os.environ['CDC_POSTGRES_PORT']
        del os.environ['CDC_METRICS_ENABLED']
    
    def test_missing_required_field(self):
        invalid_config = {'kafka': {'topics': ['topic1']}}
        
        with open(self.config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        loader = ConfigLoader(str(self.config_file))
        
        with self.assertRaises(ConfigurationException) as context:
            loader.load()
        
        self.assertIn('kafka.bootstrap_servers', str(context.exception))
    
    def test_invalid_type(self):
        invalid_config = self.valid_config.copy()
        invalid_config['kafka']['topics'] = 'not_a_list'
        
        with open(self.config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        loader = ConfigLoader(str(self.config_file))
        
        with self.assertRaises(ConfigurationException) as context:
            loader.load()
        
        self.assertIn('Invalid type', str(context.exception))
    
    def test_get_nested_value(self):
        loader = ConfigLoader(str(self.config_file))
        config = loader.load()
        
        self.assertEqual(loader.get('kafka.bootstrap_servers'), 'localhost:9092')
        self.assertEqual(loader.get('postgres.port'), 5432)
        self.assertIsNone(loader.get('non.existent.path'))
        self.assertEqual(loader.get('non.existent.path', 'default'), 'default')
    
    def test_deep_merge(self):
        loader = ConfigLoader(str(self.config_file))
        config = loader.load()
        
        self.assertIn('retry', config['kafka'])
        self.assertEqual(config['kafka']['retry']['max_retries'], 3)
        self.assertEqual(config['kafka']['group_id'], 'test_group')

if __name__ == '__main__':
    unittest.main()