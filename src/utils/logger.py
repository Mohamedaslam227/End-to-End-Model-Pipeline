from loguru import logger
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

class LoggerConfig:
    def __init__(
        self,
        console_level: str = "INFO",
        file_level: str = "DEBUG",
        log_dir: str = "logs",
        log_format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation: str = "10 MB",
        retention: str = "7 days",
        compression: str = "zip"
    ):
        self.console_level = console_level
        self.file_level = file_level
        self.log_dir = Path(log_dir)
        self.log_format = log_format
        self.rotation = rotation
        self.retention = retention
        self.compression = compression

class PipelineLogger:
    """
    Centralized logger for ML pipeline with Singleton pattern.
    Handles console and file logging with automatic rotation and retention.
    """
    _instance: Optional['PipelineLogger'] = None
    _logger = None
    
    def __new__(cls, config: Optional[LoggerConfig] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[LoggerConfig] = None):
        if self._logger is None:
            self.config = config or LoggerConfig()
            self._setup_logger()
    
    def _setup_logger(self) -> None:
        logger.remove()
        
        logger.add(
            sys.stdout,
            format=self.config.log_format,
            level=self.config.console_level,
            colorize=True
        )
        
        self._create_log_directories()
        self._add_file_handlers()
        self._logger = logger
    
    def _create_log_directories(self) -> None:
        directories = [
            self.config.log_dir / "debug",
            self.config.log_dir / "info",
            self.config.log_dir / "error"
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _add_file_handlers(self) -> None:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        logger.add(
            self.config.log_dir / "debug" / f"debug_pipeline_{timestamp}.log",
            format=self.config.log_format,
            level="DEBUG",
            rotation=self.config.rotation,
            retention=self.config.retention,
            compression=self.config.compression
        )
        
        logger.add(
            self.config.log_dir / "info" / f"info_pipeline_{timestamp}.log",
            format=self.config.log_format,
            level="INFO",
            rotation=self.config.rotation,
            retention=self.config.retention,
            compression=self.config.compression
        )
        
        logger.add(
            self.config.log_dir / "error" / f"error_pipeline_{timestamp}.log",
            format=self.config.log_format,
            level="ERROR",
            rotation=self.config.rotation,
            retention=self.config.retention,
            compression=self.config.compression
        )
    
    def get_logger(self):
        return self._logger
    
    @classmethod
    def reset(cls) -> None:
        cls._instance = None
        cls._logger = None

def get_logger(config: Optional[LoggerConfig] = None):
    pipeline_logger = PipelineLogger(config=config)
    return pipeline_logger.get_logger()
