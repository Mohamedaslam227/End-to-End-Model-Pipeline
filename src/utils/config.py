import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

class ConfigLoader(ABC):
    @abstractmethod
    def load(self, path: Path) -> Dict[str, Any]:
        pass

class ConfigUpdater(ABC):
    def __init__(self, config_path: Path):
        self.config_path = config_path

    @abstractmethod
    def update(self, key_path: str, value: Any) -> None:
        pass

class YAMLConfigLoader(ConfigLoader):
    def load(self, path: Path) -> Dict[str, Any]:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

class JSONConfigLoader(ConfigLoader):
    def load(self, path: Path) -> Dict[str, Any]:
        with open(path, 'r') as f:
            return json.load(f)

class YAMLConfigUpdater(ConfigUpdater):
    def update(self, key_path: str, value: Any) -> None:
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        keys = key_path.split('.')
        target = config
        for key in keys[:-1]:
            target = target.setdefault(key, {})
        target[keys[-1]] = value
        
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f)

class JSONConfigUpdater(ConfigUpdater):
    def update(self, key_path: str, value: Any) -> None:
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        keys = key_path.split('.')
        target = config
        for key in keys[:-1]:
            target = target.setdefault(key, {})
        target[keys[-1]] = value
        
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=4)

class ConfigManager:
    """
    Configuration Manager with Singleton pattern.
    Manages application configuration with multiple file format support.
    """
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        if config_path and not self._config:
            self.config_path = Path(config_path)
            self._validate_path()
            self._loader = self._get_loader()
            self._updater = self._get_updater()
            self._config = self._load_config()
    
    def _validate_path(self) -> None:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
    
    def _get_loader(self) -> ConfigLoader:
        extension = self.config_path.suffix.lower()
        loaders = {
            '.yaml': YAMLConfigLoader(),
            '.yml': YAMLConfigLoader(),
            '.json': JSONConfigLoader(),
        }
        if extension not in loaders:
            raise ValueError(f"Unsupported config file format: {extension}")
        return loaders[extension]
    
    def _get_updater(self) -> ConfigUpdater:
        extension = self.config_path.suffix.lower()
        updaters = {
            '.yaml': YAMLConfigUpdater(self.config_path),
            '.yml': YAMLConfigUpdater(self.config_path),
            '.json': JSONConfigUpdater(self.config_path),
        }
        if extension not in updaters:
            raise ValueError(f"Unsupported config file format: {extension}")
        return updaters[extension]
    
    def _load_config(self) -> Dict[str, Any]:
        return self._loader.load(self.config_path)
    
    @property
    def config(self) -> Dict[str, Any]:
        return self._config
    
    def get(self, key_path: str, default: Any = None) -> Any:
        keys = key_path.split('.')
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    
    def update(self, key_path: str, value: Any) -> None:
        """Update configuration value and save to file"""
        self._updater.update(key_path, value)
        self.reload() # Refresh in-memory config
    
    def reload(self) -> None:
        self._config = self._load_config()