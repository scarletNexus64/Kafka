import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BaseComponent(ABC):
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._running = False
        self._metrics = {}
    
    @abstractmethod
    def start(self) -> None:
        pass
    
    @abstractmethod
    def stop(self) -> None:
        pass
    
    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        pass
    
    def get_metrics(self) -> Dict[str, Any]:
        return self._metrics.copy()
    
    def is_running(self) -> bool:
        return self._running