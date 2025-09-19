import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
import json

from ..core.exceptions import ConfigurationException

class ConfigLoader:
    def __init__(self, config_path: Optional[str] = None, env_prefix: str = "CDC"):
        self.config_path = config_path or self._find_config_file()
        self.env_prefix = env_prefix
        self._config = {}
        self._schema = {}
    
    def _find_config_file(self) -> str:
        search_paths = [
            'config/config.yaml',
            'config/config.yml',
            'config.yaml',
            'config.yml',
            '/etc/cdc-sink/config.yaml'
        ]
        
        for path in search_paths:
            if Path(path).exists():
                return path
        
        raise ConfigurationException(
            "No configuration file found",
            error_code="CONFIG_NOT_FOUND",
            details={'search_paths': search_paths}
        )
    
    def load(self) -> Dict[str, Any]:
        self._config = self._load_file()
        self._apply_env_overrides()
        self._validate_config()
        self._apply_defaults()
        return self._config
    
    def _load_file(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, 'r') as f:
                if self.config_path.endswith('.json'):
                    return json.load(f)
                else:
                    return yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationException(
                f"Failed to load configuration file: {e}",
                error_code="CONFIG_LOAD_ERROR",
                details={'path': self.config_path}
            )
    
    def _apply_env_overrides(self):
        env_mappings = {
            f'{self.env_prefix}_KAFKA_BOOTSTRAP_SERVERS': 'kafka.bootstrap_servers',
            f'{self.env_prefix}_KAFKA_GROUP_ID': 'kafka.group_id',
            f'{self.env_prefix}_POSTGRES_HOST': 'postgres.host',
            f'{self.env_prefix}_POSTGRES_PORT': 'postgres.port',
            f'{self.env_prefix}_POSTGRES_DATABASE': 'postgres.database',
            f'{self.env_prefix}_POSTGRES_USER': 'postgres.user',
            f'{self.env_prefix}_POSTGRES_PASSWORD': 'postgres.password',
            f'{self.env_prefix}_LOG_LEVEL': 'logging.level',
            f'{self.env_prefix}_METRICS_ENABLED': 'metrics.enabled',
            f'{self.env_prefix}_METRICS_PORT': 'metrics.port',
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                self._set_nested_value(config_path, value)
    
    def _set_nested_value(self, path: str, value: Any):
        keys = path.split('.')
        current = self._config
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        if value.lower() in ('true', 'false'):
            value = value.lower() == 'true'
        elif value.isdigit():
            value = int(value)
        
        current[keys[-1]] = value
    
    def _validate_config(self):
        required_fields = [
            ('kafka.bootstrap_servers', str),
            ('kafka.topics', list),
            ('postgres.host', str),
            ('postgres.user', str),
            ('postgres.password', str),
            ('postgres.database', str),
        ]
        
        for field_path, expected_type in required_fields:
            value = self._get_nested_value(field_path)
            if value is None:
                raise ConfigurationException(
                    f"Required configuration field missing: {field_path}",
                    error_code="MISSING_FIELD",
                    details={'field': field_path}
                )
            
            if not isinstance(value, expected_type):
                raise ConfigurationException(
                    f"Invalid type for {field_path}: expected {expected_type.__name__}",
                    error_code="INVALID_TYPE",
                    details={'field': field_path, 'expected': expected_type.__name__}
                )
    
    def _get_nested_value(self, path: str) -> Any:
        keys = path.split('.')
        current = self._config
        
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        
        return current
    
    def _apply_defaults(self):
        defaults = {
            'kafka': {
                'group_id': 'cdc_consumer',
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': False,
                'session_timeout_ms': 30000,
                'heartbeat_interval_ms': 10000,
                'max_poll_interval_ms': 300000,
                'batch_size': 100,
                'batch_timeout_ms': 1000,
                'retry': {
                    'max_retries': 3,
                    'backoff_ms': 1000,
                    'max_backoff_ms': 30000
                }
            },
            'postgres': {
                'port': 5432,
                'min_connections': 1,
                'max_connections': 10,
                'connect_timeout': 10,
                'continue_on_error': True,
                'batch': {
                    'size': 100,
                    'timeout_ms': 1000
                },
                'retry': {
                    'max_retries': 3,
                    'backoff_ms': 1000,
                    'max_backoff_ms': 30000
                }
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': None
            },
            'metrics': {
                'enabled': True,
                'port': 8080,
                'path': '/metrics'
            },
            'processing': {
                'max_workers': 4,
                'queue_size': 1000,
                'shutdown_timeout': 30
            }
        }
        
        self._config = self._deep_merge(defaults, self._config)
    
    def _deep_merge(self, default: Dict, override: Dict) -> Dict:
        result = default.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get(self, path: str, default: Any = None) -> Any:
        value = self._get_nested_value(path)
        return value if value is not None else default
    
    def reload(self):
        self._config = self.load()
        
    def save_schema(self, path: str):
        schema = {
            'kafka': {
                'type': 'object',
                'required': ['bootstrap_servers', 'topics'],
                'properties': {
                    'bootstrap_servers': {'type': 'string'},
                    'topics': {'type': 'array', 'items': {'type': 'string'}},
                    'group_id': {'type': 'string'},
                }
            },
            'postgres': {
                'type': 'object',
                'required': ['host', 'user', 'password', 'database'],
                'properties': {
                    'host': {'type': 'string'},
                    'port': {'type': 'integer'},
                    'database': {'type': 'string'},
                    'user': {'type': 'string'},
                    'password': {'type': 'string'},
                }
            }
        }
        
        with open(path, 'w') as f:
            json.dump(schema, f, indent=2)