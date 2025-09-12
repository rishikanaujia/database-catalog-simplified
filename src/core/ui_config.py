"""UI configuration loader"""

import yaml
from pathlib import Path
from typing import Dict, Any

class UIConfig:
    """Loads and manages UI configuration"""
    
    def __init__(self, config_path: str = "config/ui_settings.yaml"):
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback to defaults"""
        if not self.config_path.exists():
            print(f"Warning: Config file {self.config_path} not found. Using default UI settings.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                print(f"Loaded UI settings from {self.config_path}")
                return config
        except Exception as e:
            print(f"Error loading UI config: {e}. Using defaults.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults"""
        return {
            'server': {
                'host': '0.0.0.0',
                'port': 7860,
                'share': False,
                'inbrowser': True,
                'max_threads': 40,
                'show_error': True,
                'show_api': True
            },
            'display': {
                'results_per_page': 50,
                'max_search_results': 200,
                'description_truncate_length': 100,
                'sample_values_truncate_length': 150,
                'long_text_indicator': '...',
                'max_columns_in_context': 10,
                'show_row_numbers': True,
                'enable_sorting': True
            },
            'styling': {
                'table_border': '1px solid #ddd',
                'cell_padding': '8px',
                'header_background': '#f2f2f2',
                'header_text_color': '#333',
                'even_row_background': '#f9f9f9',
                'odd_row_background': '#ffffff',
                'hover_background': '#e6f3ff',
                'font_family': 'Arial, sans-serif',
                'font_size': '14px',
                'header_font_weight': 'bold',
                'code_background': '#f5f5f5',
                'code_border': '1px solid #ccc',
                'code_padding': '2px 4px'
            },
            'interface': {
                'default_tab': 'Search Catalog',
                'search_placeholder': 'Search tables, columns, or descriptions...',
                'enable_advanced_search': True,
                'search_history_size': 10,
                'show_table_stats': True,
                'enable_column_filtering': True,
                'enable_csv_export': True,
                'enable_json_export': False
            },
            'theme': {
                'primary_color': 'blue',
                'secondary_color': 'yellow',
                'neutral_color': 'gray',
                'success_color': 'green',
                'warning_color': 'orange',
                'error_color': 'red',
                'enable_dark_mode': False,
                'dark_background': '#1a1a1a',
                'dark_text': '#ffffff'
            },
            'performance': {
                'lazy_loading': True,
                'pagination_enabled': True,
                'debounce_search_ms': 300,
                'cache_search_results': True,
                'cache_duration_minutes': 30
            }
        }
    
    # Server configuration properties
    @property
    def host(self) -> str:
        return self._config['server']['host']
    
    @property
    def port(self) -> int:
        return self._config['server']['port']
    
    @property
    def share(self) -> bool:
        return self._config['server']['share']
    
    @property
    def inbrowser(self) -> bool:
        return self._config['server']['inbrowser']
    
    @property
    def max_threads(self) -> int:
        return self._config['server']['max_threads']
    
    @property
    def show_error(self) -> bool:
        return self._config['server']['show_error']
    
    @property
    def show_api(self) -> bool:
        return self._config['server']['show_api']
    
    # Display configuration properties
    @property
    def results_per_page(self) -> int:
        return self._config['display']['results_per_page']
    
    @property
    def max_search_results(self) -> int:
        return self._config['display']['max_search_results']
    
    @property
    def description_truncate_length(self) -> int:
        return self._config['display']['description_truncate_length']
    
    @property
    def sample_values_truncate_length(self) -> int:
        return self._config['display']['sample_values_truncate_length']
    
    @property
    def long_text_indicator(self) -> str:
        return self._config['display']['long_text_indicator']
    
    @property
    def max_columns_in_context(self) -> int:
        return self._config['display']['max_columns_in_context']
    
    # Styling properties
    @property
    def table_border(self) -> str:
        return self._config['styling']['table_border']
    
    @property
    def cell_padding(self) -> str:
        return self._config['styling']['cell_padding']
    
    @property
    def header_background(self) -> str:
        return self._config['styling']['header_background']
    
    @property
    def header_text_color(self) -> str:
        return self._config['styling']['header_text_color']
    
    @property
    def font_family(self) -> str:
        return self._config['styling']['font_family']
    
    @property
    def font_size(self) -> str:
        return self._config['styling']['font_size']
    
    # Interface properties
    @property
    def search_placeholder(self) -> str:
        return self._config['interface']['search_placeholder']
    
    @property
    def default_tab(self) -> str:
        return self._config['interface']['default_tab']
    
    @property
    def enable_csv_export(self) -> bool:
        return self._config['interface']['enable_csv_export']
    
    # Theme properties
    @property
    def primary_color(self) -> str:
        return self._config['theme']['primary_color']
    
    @property
    def success_color(self) -> str:
        return self._config['theme']['success_color']
    
    @property
    def enable_dark_mode(self) -> bool:
        return self._config['theme']['enable_dark_mode']
    
    # Helper methods
    def get_table_style(self) -> str:
        """Generate CSS style for tables"""
        return f"""
        width: 100%; 
        border-collapse: collapse; 
        font-family: {self.font_family}; 
        font-size: {self.font_size};
        """
    
    def get_header_style(self) -> str:
        """Generate CSS style for table headers"""
        return f"""
        border: {self.table_border}; 
        padding: {self.cell_padding}; 
        background-color: {self.header_background}; 
        color: {self.header_text_color}; 
        font-weight: {self._config['styling']['header_font_weight']};
        """
    
    def get_cell_style(self, is_code: bool = False) -> str:
        """Generate CSS style for table cells"""
        base_style = f"""
        border: {self.table_border}; 
        padding: {self.cell_padding};
        """
        
        if is_code:
            base_style += f"""
            background-color: {self._config['styling']['code_background']}; 
            border: {self._config['styling']['code_border']}; 
            padding: {self._config['styling']['code_padding']};
            font-family: monospace;
            """
        
        return base_style
    
    def truncate_text(self, text: str, max_length: int = None, is_description: bool = False) -> str:
        """Truncate text according to configuration"""
        if max_length is None:
            max_length = self.description_truncate_length if is_description else self.sample_values_truncate_length
        
        if len(text) <= max_length:
            return text
        
        return text[:max_length] + self.long_text_indicator
    
    def get_gradio_theme_config(self) -> Dict[str, Any]:
        """Get Gradio theme configuration - FIXED to use valid color names"""
        return {
            'primary_hue': self._config['theme']['primary_color'],
            'secondary_hue': self._config['theme']['secondary_color'],
            'neutral_hue': self._config['theme'].get('neutral_color', 'gray')
        }

# Global instance
UI_CONFIG = UIConfig()