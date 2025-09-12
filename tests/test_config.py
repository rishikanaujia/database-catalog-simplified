"""Tests for configuration classes"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch

# Add src to path for testing - robust path handling
import sys
import os

# Get the project root directory (parent of tests directory)
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'

# Add both project root and src to Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(src_path))

# Debug: Print paths to help troubleshoot
print(f"Project root: {project_root}")
print(f"Source path: {src_path}")
print(f"Source path exists: {src_path.exists()}")

from src.core.base_config import BaseConfig

class TestConfig(BaseConfig):
    """Test configuration class"""
    
    def _get_default_config(self):
        return {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'test_db'
            },
            'api': {
                'key': 'test_key',
                'timeout': 30
            },
            'features': {
                'caching': True,
                'logging_level': 'INFO'
            }
        }

class TestBaseConfig:
    """Test cases for BaseConfig"""
    
    def test_default_config_loading(self):
        """Test loading with non-existent file falls back to defaults"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "nonexistent.yaml"
            config = TestConfig(str(config_path), "test")
            
            assert config.get('database.host') == 'localhost'
            assert config.get('database.port') == 5432
            assert config.get('api.timeout') == 30
    
    def test_yaml_config_loading(self):
        """Test loading from actual YAML file"""
        test_config = {
            'database': {
                'host': 'production.db',
                'port': 3306,
                'name': 'prod_db'
            },
            'api': {
                'key': 'prod_key',
                'timeout': 60
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(test_config, f)
            config_path = f.name
        
        try:
            config = TestConfig(config_path, "test")
            assert config.get('database.host') == 'production.db'
            assert config.get('database.port') == 3306
            assert config.get('api.timeout') == 60
            # Should fall back to default for missing values
            assert config.get('features.caching') == True
        finally:
            Path(config_path).unlink()
    
    def test_dot_notation_access(self):
        """Test getting values with dot notation"""
        config = TestConfig("nonexistent.yaml", "test")
        
        assert config.get('database.host') == 'localhost'
        assert config.get('database.port') == 5432
        assert config.get('nonexistent.key', 'default') == 'default'
        assert config.get('database.nonexistent', 'default') == 'default'
    
    def test_config_update(self):
        """Test updating configuration values"""
        config = TestConfig("nonexistent.yaml", "test")
        
        # Test simple update
        config.update('api.timeout', 120)
        assert config.get('api.timeout') == 120
        
        # Test nested update
        config.update('database.host', 'updated.host')
        assert config.get('database.host') == 'updated.host'
        
        # Test creating new nested keys
        config.update('new.nested.key', 'value')
        assert config.get('new.nested.key') == 'value'
    
    def test_config_save_and_reload(self):
        """Test saving and reloading configuration"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test_config.yaml"
            
            # Create and modify config
            config = TestConfig(str(config_path), "test")
            config.update('api.timeout', 999)
            config.update('new.setting', 'test_value')
            
            # Save configuration
            assert config.save() == True
            assert config_path.exists()
            
            # Create new instance to test loading
            config2 = TestConfig(str(config_path), "test")
            assert config2.get('api.timeout') == 999
            assert config2.get('new.setting') == 'test_value'
            # Default values should still be present
            assert config2.get('database.host') == 'localhost'
    
    def test_invalid_yaml_handling(self):
        """Test handling of invalid YAML files"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [unclosed")
            config_path = f.name
        
        try:
            config = TestConfig(config_path, "test")
            # Should fall back to defaults on YAML error
            assert config.get('database.host') == 'localhost'
        finally:
            Path(config_path).unlink()
    
    def test_empty_yaml_handling(self):
        """Test handling of empty YAML files"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")  # Empty file
            config_path = f.name
        
        try:
            config = TestConfig(config_path, "test")
            # Should fall back to defaults on empty file
            assert config.get('database.host') == 'localhost'
        finally:
            Path(config_path).unlink()

def test_component_configs():
    """Test that existing config classes can be imported without errors"""
    try:
        from src.core.ai_config import AI_CONFIG
        from src.core.data_processing_config import DATA_PROCESSING_CONFIG
        from src.core.ui_config import UI_CONFIG
        from src.core.column_config import COLUMN_CONFIG
        
        # Test basic access to verify configs loaded
        assert AI_CONFIG.primary_model is not None
        assert DATA_PROCESSING_CONFIG.max_sample_rows > 0
        assert UI_CONFIG.port > 0
        assert len(COLUMN_CONFIG.primary_key_suffixes) > 0
        
        print("✅ All configuration classes imported successfully")
        
    except Exception as e:
        pytest.fail(f"Failed to import configuration classes: {e}")

if __name__ == "__main__":
    # Run tests directly
    test_component_configs()
    print("✅ Configuration tests completed")