"""Simple configuration loader for column classification rules"""

import yaml
from pathlib import Path
from typing import Dict, List, Any

class ColumnClassificationConfig:
    """Loads and manages column classification rules"""
    
    def __init__(self, config_path: str = "config/column_classification.yaml"):
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback to defaults"""
        if not self.config_path.exists():
            print(f"Warning: Config file {self.config_path} not found. Using default rules.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                print(f"Loaded column classification rules from {self.config_path}")
                return config
        except Exception as e:
            print(f"Error loading config file: {e}. Using default rules.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults if config file is missing"""
        return {
            'primary_keys': {
                'suffixes': ['_sk', '_id', '_key', '_pk'],
                'requires_not_null': True
            },
            'foreign_keys': {
                'suffixes': ['_sk', '_fk', '_ref_id'],
                'requires_not_null': False
            },
            'measures': {
                'amount_fields': {
                    'keywords': ['amount', 'price', 'cost', 'total', 'value', 'revenue', 'sales'],
                    'business_type': 'Currency'
                },
                'quantity_fields': {
                    'keywords': ['quantity', 'qty', 'count', 'num', 'units'],
                    'business_type': 'Quantity'
                }
            },
            'dimensions': {
                'description_fields': {
                    'keywords': ['name', 'desc', 'description', 'title', 'label'],
                    'business_type': 'Description'
                },
                'status_fields': {
                    'keywords': ['status', 'state', 'flag', 'indicator', 'type'],
                    'business_type': 'Status'
                }
            },
            'default_business_types': {
                'DATE': 'Date',
                'DATETIME': 'Date',
                'TIMESTAMP': 'Date',
                'TEXT': 'Text',
                'VARCHAR': 'Text',
                'NUMBER': 'Numeric',
                'INTEGER': 'Numeric'
            }
        }
    
    # Convenience properties for easy access
    @property
    def primary_key_suffixes(self) -> List[str]:
        return self._config['primary_keys']['suffixes']
    
    @property
    def foreign_key_suffixes(self) -> List[str]:
        return self._config['foreign_keys']['suffixes']
    
    @property
    def primary_key_requires_not_null(self) -> bool:
        return self._config['primary_keys']['requires_not_null']
    
    @property
    def amount_keywords(self) -> List[str]:
        return self._config['measures']['amount_fields']['keywords']
    
    @property
    def quantity_keywords(self) -> List[str]:
        return self._config['measures']['quantity_fields']['keywords']
    
    @property
    def description_keywords(self) -> List[str]:
        return self._config['dimensions']['description_fields']['keywords']
    
    @property
    def status_keywords(self) -> List[str]:
        return self._config['dimensions'].get('status_fields', {}).get('keywords', [])
    
    @property
    def location_keywords(self) -> List[str]:
        return self._config['dimensions'].get('location_fields', {}).get('keywords', [])
    
    def get_business_type_for_sql_type(self, sql_type: str) -> str:
        """Get default business type for SQL data type"""
        return self._config['default_business_types'].get(sql_type.upper(), 'Text')
    
    def get_amount_business_type(self) -> str:
        return self._config['measures']['amount_fields']['business_type']
    
    def get_quantity_business_type(self) -> str:
        return self._config['measures']['quantity_fields']['business_type']
    
    def get_description_business_type(self) -> str:
        return self._config['dimensions']['description_fields']['business_type']
    
    def get_status_business_type(self) -> str:
        return self._config['dimensions'].get('status_fields', {}).get('business_type', 'Status')
    
    def get_location_business_type(self) -> str:
        return self._config['dimensions'].get('location_fields', {}).get('business_type', 'Location')

# Global instance
COLUMN_CONFIG = ColumnClassificationConfig()