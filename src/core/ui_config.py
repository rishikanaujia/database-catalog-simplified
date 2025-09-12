"""UI configuration loader - Refactored to use BaseConfig"""

from typing import Dict, Any
from src.core.base_config import BaseConfig

class UIConfig(BaseConfig):
    """Loads and manages UI configuration using BaseConfig"""
    
    def __init__(self, config_path: str = "config/ui_settings.yaml"):
        super().__init__(config_path, "UI settings")
    
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
        return self.get('server.host', '0.0.0.0')
    
    @property
    def port(self) -> int:
        return self.get('server.port', 7860)
    
    @property
    def share(self) -> bool:
        return self.get('server.share', False)
    
    @property
    def inbrowser(self) -> bool:
        return self.get('server.inbrowser', True)
    
    @property
    def max_threads(self) -> int:
        return self.get('server.max_threads', 40)
    
    @property
    def show_error(self) -> bool:
        return self.get('server.show_error', True)
    
    @property
    def show_api(self) -> bool:
        return self.get('server.show_api', True)
    
    # Display configuration properties
    @property
    def results_per_page(self) -> int:
        return self.get('display.results_per_page', 50)
    
    @property
    def max_search_results(self) -> int:
        return self.get('display.max_search_results', 200)
    
    @property
    def description_truncate_length(self) -> int:
        return self.get('display.description_truncate_length', 100)
    
    @property
    def sample_values_truncate_length(self) -> int:
        return self.get('display.sample_values_truncate_length', 150)
    
    @property
    def long_text_indicator(self) -> str:
        return self.get('display.long_text_indicator', '...')
    
    @property
    def max_columns_in_context(self) -> int:
        return self.get('display.max_columns_in_context', 10)
    
    # Styling properties
    @property
    def table_border(self) -> str:
        return self.get('styling.table_border', '1px solid #ddd')
    
    @property
    def cell_padding(self) -> str:
        return self.get('styling.cell_padding', '8px')
    
    @property
    def header_background(self) -> str:
        return self.get('styling.header_background', '#f2f2f2')
    
    @property
    def header_text_color(self) -> str:
        return self.get('styling.header_text_color', '#333')
    
    @property
    def font_family(self) -> str:
        return self.get('styling.font_family', 'Arial, sans-serif')
    
    @property
    def font_size(self) -> str:
        return self.get('styling.font_size', '14px')
    
    # Interface properties
    @property
    def search_placeholder(self) -> str:
        return self.get('interface.search_placeholder', 'Search tables, columns, or descriptions...')
    
    @property
    def default_tab(self) -> str:
        return self.get('interface.default_tab', 'Search Catalog')
    
    @property
    def enable_csv_export(self) -> bool:
        return self.get('interface.enable_csv_export', True)
    
    # Theme properties
    @property
    def primary_color(self) -> str:
        return self.get('theme.primary_color', 'blue')
    
    @property
    def success_color(self) -> str:
        return self.get('theme.success_color', 'green')
    
    @property
    def enable_dark_mode(self) -> bool:
        return self.get('theme.enable_dark_mode', False)
    
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
        header_font_weight = self.get('styling.header_font_weight', 'bold')
        return f"""
        border: {self.table_border}; 
        padding: {self.cell_padding}; 
        background-color: {self.header_background}; 
        color: {self.header_text_color}; 
        font-weight: {header_font_weight};
        """
    
    def get_cell_style(self, is_code: bool = False) -> str:
        """Generate CSS style for table cells"""
        base_style = f"""
        border: {self.table_border}; 
        padding: {self.cell_padding};
        """
        
        if is_code:
            code_background = self.get('styling.code_background', '#f5f5f5')
            code_border = self.get('styling.code_border', '1px solid #ccc')
            code_padding = self.get('styling.code_padding', '2px 4px')
            
            base_style += f"""
            background-color: {code_background}; 
            border: {code_border}; 
            padding: {code_padding};
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
        """Get Gradio theme configuration - Using BaseConfig dot notation"""
        return {
            'primary_hue': self.get('theme.primary_color', 'blue'),
            'secondary_hue': self.get('theme.secondary_color', 'yellow'),
            'neutral_hue': self.get('theme.neutral_color', 'gray')
        }
    
    def validate(self) -> bool:
        """Validate configuration values"""
        try:
            # Validate port range
            if not (1024 <= self.port <= 65535):
                return False
            
            # Validate numeric values
            if self.results_per_page <= 0:
                return False
            if self.max_search_results <= 0:
                return False
            if self.max_threads <= 0:
                return False
            
            # Validate theme colors
            valid_colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'pink', 'gray']
            if self.primary_color not in valid_colors:
                return False
            
            return True
        except Exception:
            return False

# Global instance
UI_CONFIG = UIConfig()