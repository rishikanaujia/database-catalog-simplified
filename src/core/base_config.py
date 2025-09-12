"""Base configuration loader to reduce duplication"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger("database_catalog")

class BaseConfig(ABC):
    """Base configuration loader with common functionality"""
    
    def __init__(self, config_path: str, config_name: str):
        self.config_path = Path(config_path)
        self.config_name = config_name
        self._config = self._load_config()
        logger.info(f"Loaded {config_name} configuration")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback to defaults"""
        if not self.config_path.exists():
            logger.warning(f"Config file {self.config_path} not found. Using default {self.config_name} settings.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if config is None:
                    logger.warning(f"Empty config file {self.config_path}. Using defaults.")
                    return self._get_default_config()
                
                logger.info(f"Loaded {self.config_name} settings from {self.config_path}")
                return config
                
        except yaml.YAMLError as e:
            logger.error(f"YAML error in {self.config_path}: {e}. Using default settings.")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading {self.config_name} config from {self.config_path}: {e}. Using defaults.")
            return self._get_default_config()
    
    @abstractmethod
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration - must be implemented by subclasses"""
        pass
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'database.host')"""
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def update(self, key_path: str, value: Any) -> None:
        """Update configuration value using dot notation"""
        keys = key_path.split('.')
        config_section = self._config
        
        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in config_section:
                config_section[key] = {}
            config_section = config_section[key]
        
        # Set the final value
        config_section[keys[-1]] = value
        logger.debug(f"Updated {self.config_name} config: {key_path} = {value}")
    
    def validate(self) -> bool:
        """Validate configuration - can be overridden by subclasses"""
        return True
    
    def save(self, output_path: Optional[str] = None) -> bool:
        """Save current configuration to file"""
        save_path = Path(output_path) if output_path else self.config_path
        
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._config, f, default_flow_style=False, indent=2)
            
            logger.info(f"Saved {self.config_name} configuration to {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save {self.config_name} config to {save_path}: {e}")
            return False
    
    def reload(self) -> bool:
        """Reload configuration from file"""
        try:
            old_config = self._config.copy()
            self._config = self._load_config()
            
            if self.validate():
                logger.info(f"Reloaded {self.config_name} configuration")
                return True
            else:
                logger.error(f"Validation failed after reloading {self.config_name}")
                self._config = old_config
                return False
                
        except Exception as e:
            logger.error(f"Failed to reload {self.config_name} config: {e}")
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Return a copy of the current configuration"""
        return self._config.copy()
    
    def __str__(self) -> str:
        return f"{self.config_name}Config(path={self.config_path})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config_path='{self.config_path}', config_name='{self.config_name}')"