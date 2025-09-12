"""Data processing configuration loader - Refactored to use BaseConfig"""

from typing import Dict, List, Any
from src.core.base_config import BaseConfig

class DataProcessingConfig(BaseConfig):
    """Loads and manages data processing configuration using BaseConfig"""
    
    def __init__(self, config_path: str = "config/data_processing.yaml"):
        super().__init__(config_path, "data processing")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults"""
        return {
            'table_selection': {
                'mode': 'selected',
                'include_tables': ['CATALOG_PAGE'],
                'exclude_tables': ['LARGE_FACT_TABLE', 'TEMP_TABLE']
            },
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
    
    # Table selection properties
    @property
    def table_selection_mode(self) -> str:
        return self.get('table_selection.mode', 'selected')
    
    @property
    def include_tables(self) -> List[str]:
        return self.get('table_selection.include_tables', ['CATALOG_PAGE'])
    
    @property
    def exclude_tables(self) -> List[str]:
        return self.get('table_selection.exclude_tables', [])
    
    # Sampling configuration properties
    @property
    def default_sample_size(self) -> int:
        return self.get('sampling.default_sample_size', 0)
    
    @property
    def small_table_threshold(self) -> int:
        return self.get('sampling.small_table_threshold', 999999999)
    
    @property
    def medium_table_threshold(self) -> int:
        return self.get('sampling.medium_table_threshold', 100000)
    
    @property
    def large_table_threshold(self) -> int:
        return self.get('sampling.large_table_threshold', 1000000)
    
    @property
    def bernoulli_sample_percent(self) -> int:
        return self.get('sampling.bernoulli_sample_percent', 10)
    
    @property
    def max_sample_rows(self) -> int:
        return self.get('sampling.max_sample_rows', 5000)
    
    # Profiling configuration properties
    @property
    def max_distinct_values(self) -> int:
        return self.get('profiling.max_distinct_values', 50)
    
    @property
    def batch_size(self) -> int:
        return self.get('profiling.batch_size', 10)
    
    @property
    def column_timeout_seconds(self) -> int:
        return self.get('profiling.column_timeout_seconds', 30)
    
    @property
    def query_timeout_seconds(self) -> int:
        return self.get('profiling.query_timeout_seconds', 120)
    
    # Text analysis properties
    @property
    def max_sample_text_length(self) -> int:
        return self.get('text_analysis.max_sample_text_length', 200)
    
    @property
    def max_individual_value_length(self) -> int:
        return self.get('text_analysis.max_individual_value_length', 50)
    
    @property
    def truncation_indicator(self) -> str:
        return self.get('text_analysis.truncation_indicator', ' ... ')
    
    # Numeric analysis properties
    @property
    def decimal_places(self) -> int:
        return self.get('numeric_analysis.decimal_places', 2)
    
    @property
    def calculate_percentiles(self) -> bool:
        return self.get('numeric_analysis.calculate_percentiles', False)
    
    @property
    def percentiles(self) -> List[int]:
        return self.get('numeric_analysis.percentiles', [25, 50, 75, 90, 95])
    
    # Performance properties
    @property
    def enable_parallel_processing(self) -> bool:
        return self.get('performance.enable_parallel_processing', False)
    
    @property
    def max_workers(self) -> int:
        return self.get('performance.max_workers', 4)
    
    @property
    def memory_limit_mb(self) -> int:
        return self.get('performance.memory_limit_mb', 1024)
    
    @property
    def enable_caching(self) -> bool:
        return self.get('performance.enable_caching', True)
    
    # Helper methods
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
    
    def validate(self) -> bool:
        """Validate configuration values"""
        try:
            # Validate table selection mode
            valid_modes = ['all', 'selected']
            if self.table_selection_mode not in valid_modes:
                return False
            
            # Validate numeric values
            if self.max_sample_rows <= 0:
                return False
            if self.batch_size <= 0:
                return False
            if self.decimal_places < 0:
                return False
            
            return True
        except Exception:
            return False

# Global instance
DATA_PROCESSING_CONFIG = DataProcessingConfig()