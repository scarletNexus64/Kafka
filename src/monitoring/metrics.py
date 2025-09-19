from typing import Dict, Any, Optional
from collections import defaultdict
from datetime import datetime
import time
import threading
import json

class MetricsCollector:
    def __init__(self):
        self._metrics = defaultdict(lambda: defaultdict(int))
        self._timers = {}
        self._gauges = {}
        self._lock = threading.Lock()
        self._start_time = time.time()
    
    def increment(self, metric: str, value: int = 1, tags: Dict[str, str] = None):
        with self._lock:
            key = self._build_key(metric, tags)
            self._metrics['counters'][key] += value
    
    def gauge(self, metric: str, value: float, tags: Dict[str, str] = None):
        with self._lock:
            key = self._build_key(metric, tags)
            self._gauges[key] = {
                'value': value,
                'timestamp': time.time()
            }
    
    def timer(self, metric: str, tags: Dict[str, str] = None):
        return Timer(self, metric, tags)
    
    def record_timing(self, metric: str, duration: float, tags: Dict[str, str] = None):
        with self._lock:
            key = self._build_key(metric, tags)
            if key not in self._timers:
                self._timers[key] = {
                    'count': 0,
                    'sum': 0,
                    'min': float('inf'),
                    'max': 0,
                    'last': 0
                }
            
            self._timers[key]['count'] += 1
            self._timers[key]['sum'] += duration
            self._timers[key]['min'] = min(self._timers[key]['min'], duration)
            self._timers[key]['max'] = max(self._timers[key]['max'], duration)
            self._timers[key]['last'] = duration
    
    def _build_key(self, metric: str, tags: Optional[Dict[str, str]]) -> str:
        if not tags:
            return metric
        
        tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{metric},{tag_str}"
    
    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            metrics = {
                'uptime_seconds': time.time() - self._start_time,
                'timestamp': datetime.utcnow().isoformat(),
                'counters': dict(self._metrics['counters']),
                'gauges': {k: v['value'] for k, v in self._gauges.items()},
                'timers': {}
            }
            
            for key, timer_data in self._timers.items():
                if timer_data['count'] > 0:
                    metrics['timers'][key] = {
                        'count': timer_data['count'],
                        'mean': timer_data['sum'] / timer_data['count'],
                        'min': timer_data['min'],
                        'max': timer_data['max'],
                        'last': timer_data['last']
                    }
            
            return metrics
    
    def reset(self):
        with self._lock:
            self._metrics.clear()
            self._timers.clear()
            self._gauges.clear()
    
    def export_prometheus(self) -> str:
        metrics = self.get_metrics()
        lines = []
        
        lines.append(f"# HELP uptime_seconds Time since metrics collector started")
        lines.append(f"# TYPE uptime_seconds gauge")
        lines.append(f"uptime_seconds {metrics['uptime_seconds']}")
        
        for counter, value in metrics['counters'].items():
            name = counter.replace('.', '_').replace(',', '{').replace('=', '="') + '"}'
            if '{' not in name:
                name = name.replace('}', '')
            lines.append(f"# TYPE {counter.split(',')[0].replace('.', '_')} counter")
            lines.append(f"{name} {value}")
        
        for gauge, value in metrics['gauges'].items():
            name = gauge.replace('.', '_').replace(',', '{').replace('=', '="') + '"}'
            if '{' not in name:
                name = name.replace('}', '')
            lines.append(f"# TYPE {gauge.split(',')[0].replace('.', '_')} gauge")
            lines.append(f"{name} {value}")
        
        for timer_name, timer_data in metrics['timers'].items():
            base_name = timer_name.split(',')[0].replace('.', '_')
            tags = timer_name.replace(base_name, '').replace('.', '_')
            
            for stat in ['mean', 'min', 'max', 'last']:
                lines.append(f"{base_name}_{stat}{tags} {timer_data[stat]}")
            lines.append(f"{base_name}_count{tags} {timer_data['count']}")
        
        return '\n'.join(lines)
    
    def export_json(self) -> str:
        return json.dumps(self.get_metrics(), indent=2)

class Timer:
    def __init__(self, collector: MetricsCollector, metric: str, tags: Optional[Dict[str, str]]):
        self.collector = collector
        self.metric = metric
        self.tags = tags
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.collector.record_timing(self.metric, duration, self.tags)

class HealthChecker:
    def __init__(self):
        self.components = {}
        self._last_check = {}
        self._check_interval = 30
    
    def register_component(self, name: str, component):
        self.components[name] = component
    
    def check_health(self) -> Dict[str, Any]:
        overall_status = 'healthy'
        component_status = {}
        
        for name, component in self.components.items():
            try:
                now = time.time()
                if name not in self._last_check or (now - self._last_check[name]) > self._check_interval:
                    status = component.health_check()
                    component_status[name] = status
                    self._last_check[name] = now
                    
                    if status.get('status') == 'unhealthy':
                        overall_status = 'unhealthy'
                    elif status.get('status') == 'degraded' and overall_status != 'unhealthy':
                        overall_status = 'degraded'
                else:
                    component_status[name] = {'status': 'cached'}
                    
            except Exception as e:
                component_status[name] = {
                    'status': 'error',
                    'error': str(e)
                }
                overall_status = 'unhealthy'
        
        return {
            'status': overall_status,
            'timestamp': datetime.utcnow().isoformat(),
            'components': component_status
        }