"""Data processing configuration loader - FIXED"""

import yaml
from pathlib import Path
from typing import Dict, List, Any

class DataProcessingConfig:
    """Loads and manages data processing configuration"""
    
    def __init__(self, config_path: str = "config/data_processing.yaml"):
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback to defaults"""
        if not self.config_path.exists():
            print(f"Warning: Config file {self.config_path} not found. Using default processing settings.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                print(f"Loaded data processing settings from {self.config_path}")
                return config
        except Exception as e:
            print(f"Error loading data processing config: {e}. Using defaults.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults"""
        return {
            'sampling': {
                'default_sample_size': 0,  
                'small_table_threshold': 999999999,
                'medium_table_threshold': 100000,
                'large_table_threshold': 1000000,
                'bernoulli_sample_percent': 10,
                'max_sample_rows': 5000 
            },
            'profiling': {
                'max_distinct_values': 50,
                'batch_size': 10,
                'column_timeout_seconds': 30,
                'query_timeout_seconds': 120
            },
            'text_analysis': {
                'max_sample_text_length': 200,
                'max_individual_value_length': 50,
                'truncation_indicator': ' ... '
            },
            'numeric_analysis': {
                'decimal_places': 2,
                'calculate_percentiles': False,
                'percentiles': [25, 50, 75, 90, 95]
            },
            'performance': {
                'enable_parallel_processing': False,
                'max_workers': 4,
                'memory_limit_mb': 1024,
                'enable_caching': True
            }
        }
    
    # Sampling configuration properties
    @property
    def default_sample_size(self) -> int:
        return self._config['sampling']['default_sample_size']
    
    @property
    def small_table_threshold(self) -> int:
        return self._config['sampling']['small_table_threshold']
    
    @property
    def medium_table_threshold(self) -> int:
        return self._config['sampling']['medium_table_threshold']
    
    @property
    def large_table_threshold(self) -> int:
        return self._config['sampling']['large_table_threshold']
    
    @property
    def bernoulli_sample_percent(self) -> int:
        return self._config['sampling']['bernoulli_sample_percent']
    
    @property
    def max_sample_rows(self) -> int:
        return self._config['sampling']['max_sample_rows']
    
    # Profiling configuration properties
    @property
    def max_distinct_values(self) -> int:
        return self._config['profiling']['max_distinct_values']
    
    @property
    def batch_size(self) -> int:
        return self._config['profiling']['batch_size']
    
    @property
    def column_timeout_seconds(self) -> int:
        return self._config['profiling']['column_timeout_seconds']
    
    @property
    def query_timeout_seconds(self) -> int:
        return self._config['profiling']['query_timeout_seconds']
    
    # Text analysis properties
    @property
    def max_sample_text_length(self) -> int:
        return self._config['text_analysis']['max_sample_text_length']
    
    @property
    def max_individual_value_length(self) -> int:
        return self._config['text_analysis']['max_individual_value_length']
    
    @property
    def truncation_indicator(self) -> str:
        return self._config['text_analysis']['truncation_indicator']
    
    # Numeric analysis properties
    @property
    def decimal_places(self) -> int:
        return self._config['numeric_analysis']['decimal_places']
    
    @property
    def calculate_percentiles(self) -> bool:
        return self._config['numeric_analysis']['calculate_percentiles']
    
    @property
    def percentiles(self) -> List[int]:
        return self._config['numeric_analysis']['percentiles']
    
    # Performance properties
    @property
    def enable_parallel_processing(self) -> bool:
        return self._config['performance']['enable_parallel_processing']
    
    @property
    def max_workers(self) -> int:
        return self._config['performance']['max_workers']
    
    @property
    def memory_limit_mb(self) -> int:
        return self._config['performance']['memory_limit_mb']
    
    @property
    def enable_caching(self) -> bool:
        return self._config['performance']['enable_caching']
    
    # REMOVED DUPLICATE max_sample_rows property here
    
    def get_sampling_strategy(self, row_count: int) -> str:
        """Determine sampling strategy based on table size"""
        if row_count <= self.small_table_threshold:
            return "none"
        elif row_count <= self.medium_table_threshold:
            return "percentage"
        elif row_count <= self.large_table_threshold:
            return "percentage"
        else:
            return "row_sample"
    
    def get_sample_clause(self, row_count: int) -> str:
        """Generate appropriate TABLESAMPLE clause"""
        strategy = self.get_sampling_strategy(row_count)
        
        if strategy == "none":
            return ""
        elif strategy == "percentage":
            return f"TABLESAMPLE BERNOULLI({self.bernoulli_sample_percent})"
        else:  # row_sample
            sample_size = min(self.max_sample_rows, self.default_sample_size)
            return f"TABLESAMPLE ({sample_size} ROWS)"
    
    def format_numeric_value(self, value: float) -> str:
        """Format numeric value with configured precision"""
        if value is None:
            return "NULL"
        return f"{value:.{self.decimal_places}f}"

# Global instance
DATA_PROCESSING_CONFIG = DataProcessingConfig()