from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pathlib import Path

class BasePipelineComponent(ABC):
    """
    Abstract base class for all pipeline components.
    Established using Template Method and Dependency Injection patterns.
    """
    def __init__(self, logger, config_manager):
        self.logger = logger
        self.config_manager = config_manager
        self._validate_dependencies()
    
    def _validate_dependencies(self) -> None:
        if self.logger is None:
            raise ValueError("Logger cannot be None")
        if self.config_manager is None:
            raise ValueError("Config manager cannot be None")
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass
    
    def get_config(self, key_path: str, default: Any = None) -> Any:
        return self.config_manager.get(key_path, default)
    
    def validate_path(self, path: str, must_exist: bool = False) -> Path:
        path_obj = Path(path)
        if must_exist and not path_obj.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")
        return path_obj
    
    def create_directory(self, path: str) -> Path:
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj
    
    def log_execution_start(self, component_name: str) -> None:
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Starting: {component_name}")
        self.logger.info(f"{'='*60}")
    
    def log_execution_end(self, component_name: str, success: bool = True) -> None:
        status = "✓ COMPLETED" if success else "✗ FAILED"
        self.logger.info(f"{'='*60}")
        self.logger.info(f"{status}: {component_name}")
        self.logger.info(f"{'='*60}")

class DataComponent(BasePipelineComponent):
    """Base class for data-specific pipeline components"""
    def __init__(self, logger, config_manager):
        super().__init__(logger, config_manager)
    
    def get_data_path(self, config_key: str = "dataset.data_path") -> Path:
        data_path = self.get_config(config_key)
        if data_path is None:
            raise ValueError(f"Data path not found in config: {config_key}")
        return Path(data_path)
    
    def save_data_path(self, config_key: str = "dataset.processed_data_path") -> Path:
        save_path = self.get_config(config_key)
        if save_path is None:
            raise ValueError(f"Save path not found in config: {config_key}")
        save_path_obj = Path(save_path)
        save_path_obj.parent.mkdir(parents=True, exist_ok=True)
        return save_path_obj
