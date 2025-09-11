"""AI configuration loader"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, List

class AIConfig:
    """Loads and manages AI configuration"""
    
    def __init__(self, config_path: str = "config/ai_settings.yaml"):
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback to defaults"""
        if not self.config_path.exists():
            print(f"Warning: Config file {self.config_path} not found. Using default AI settings.")
            return self._get_default_config()
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                print(f"Loaded AI settings from {self.config_path}")
                return config
        except Exception as e:
            print(f"Error loading AI config: {e}. Using defaults.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults"""
        return {
            'models': {
                'primary_model': 'claude-3-haiku-20240307',
                'fallback_model': 'claude-3-sonnet-20240229',
                'fast_model': 'claude-3-haiku-20240307',
                'detailed_model': 'claude-3-sonnet-20240229',
                'auto_select_model': False
            },
            'tokens': {
                'table_description_max': 500,
                'column_description_max': 1000,
                'batch_description_max': 1500,
                'max_context_columns': 10,
                'max_sample_values_length': 100,
                'system_message_buffer': 100
            },
            'performance': {
                'batch_size': 10,
                'max_retries': 3,
                'retry_delay_seconds': 2,
                'request_timeout_seconds': 60,
                'enable_concurrent_requests': False,
                'max_concurrent_requests': 3
            },
            'prompts': {
                'system_message': """You are an expert at translating technical database schemas into 
business-friendly documentation. You understand both data architecture and 
business needs, creating descriptions that help users discover and understand data.""",
                'table_description_template': """Provide a clear business description for the database table '{table_name}'.

Table has {column_count} columns including:
{column_info}

Provide a 2-3 sentence business description explaining:
1. What this table contains
2. Its business purpose
3. How it might be used

Keep it business-focused and accessible to non-technical users.""",
                'column_description_template': """Provide concise business descriptions for these database columns.

Table context: {table_context}

Columns:
{column_info}

For each column, provide a 1-sentence business description explaining what it contains and how it's used.
Return only the descriptions, one per line, in the same order as the columns listed above."""
            },
            'quality': {
                'require_complete_descriptions': True,
                'min_description_length': 20,
                'max_description_length': 200,
                'avoid_technical_terms': True,
                'include_usage_examples': False,
                'validate_responses': True,
                'retry_on_short_responses': True
            },
            'customization': {
                'business_domain': 'general',
                'tone': 'professional',
                'audience': 'business_users',
                'language': 'en',
                'locale': 'US',
                'use_industry_terms': False,
                'industry_glossary_file': None
            }
        }
    
    # Model configuration properties
    @property
    def primary_model(self) -> str:
        return self._config['models']['primary_model']
    
    @property
    def fallback_model(self) -> str:
        return self._config['models']['fallback_model']
    
    @property
    def fast_model(self) -> str:
        return self._config['models']['fast_model']
    
    @property
    def detailed_model(self) -> str:
        return self._config['models']['detailed_model']
    
    @property
    def auto_select_model(self) -> bool:
        return self._config['models']['auto_select_model']
    
    # Token configuration properties
    @property
    def table_description_max_tokens(self) -> int:
        return self._config['tokens']['table_description_max']
    
    @property
    def column_description_max_tokens(self) -> int:
        return self._config['tokens']['column_description_max']
    
    @property
    def batch_description_max_tokens(self) -> int:
        return self._config['tokens']['batch_description_max']
    
    @property
    def max_context_columns(self) -> int:
        return self._config['tokens']['max_context_columns']
    
    @property
    def max_sample_values_length(self) -> int:
        return self._config['tokens']['max_sample_values_length']
    
    # Performance configuration properties
    @property
    def batch_size(self) -> int:
        return self._config['performance']['batch_size']
    
    @property
    def max_retries(self) -> int:
        return self._config['performance']['max_retries']
    
    @property
    def retry_delay_seconds(self) -> int:
        return self._config['performance']['retry_delay_seconds']
    
    @property
    def request_timeout_seconds(self) -> int:
        return self._config['performance']['request_timeout_seconds']
    
    # Quality configuration properties
    @property
    def require_complete_descriptions(self) -> bool:
        return self._config['quality']['require_complete_descriptions']
    
    @property
    def min_description_length(self) -> int:
        return self._config['quality']['min_description_length']
    
    @property
    def max_description_length(self) -> int:
        return self._config['quality']['max_description_length']
    
    @property
    def validate_responses(self) -> bool:
        return self._config['quality']['validate_responses']
    
    # Customization properties
    @property
    def business_domain(self) -> str:
        return self._config['customization']['business_domain']
    
    @property
    def tone(self) -> str:
        return self._config['customization']['tone']
    
    @property
    def audience(self) -> str:
        return self._config['customization']['audience']
    
    # Add after existing properties:

    @property
    def provider(self) -> str:
        return self._config.get('models', {}).get('provider', 'anthropic')

    @property
    def openai_config(self) -> Dict[str, Any]:
        return self._config.get('models', {}).get('openai', {})

    @property
    def gemini_config(self) -> Dict[str, Any]:
        return self._config.get('models', {}).get('gemini', {})

    @property
    def azure_config(self) -> Dict[str, Any]:
        return self._config.get('models', {}).get('azure', {})

    def get_api_key_for_provider(self, provider: str = None) -> str:
        """Get API key for specified provider"""
        if provider is None:
            provider = self.provider
        
        provider_configs = {
            'anthropic': self._config.get('models', {}).get('anthropic', {}),
            'openai': self._config.get('models', {}).get('openai', {}),
            'gemini': self._config.get('models', {}).get('gemini', {}),
            'azure': self._config.get('models', {}).get('azure', {})
        }
        
        config = provider_configs.get(provider, {})
        api_key_env = config.get('api_key_env')
        
        if api_key_env:
            return os.getenv(api_key_env)
        return None
    
    # Helper methods
    def get_model_for_task(self, task_complexity: str = "standard") -> str:
        """Select appropriate model based on task complexity"""
        if self.auto_select_model:
            if task_complexity == "simple":
                return self.fast_model
            elif task_complexity == "complex":
                return self.detailed_model
            else:
                return self.primary_model
        return self.primary_model
    
    def get_table_description_prompt(self, table_name: str, column_count: int, column_info: str) -> str:
        """Generate table description prompt using template"""
        template = self._config['prompts']['table_description_template']
        
        # Check for domain-specific templates
        domain_template_key = f"{self.business_domain}_table_template"
        if domain_template_key in self._config['prompts']:
            template = self._config['prompts'][domain_template_key]
        
        return template.format(
            table_name=table_name,
            column_count=column_count,
            column_info=column_info
        )
    
    def get_column_description_prompt(self, table_context: str, column_info: str) -> str:
        """Generate column description prompt using template"""
        template = self._config['prompts']['column_description_template']
        return template.format(
            table_context=table_context,
            column_info=column_info
        )
    
    def get_system_message(self) -> str:
        """Get system message for the AI agent"""
        return self._config['prompts']['system_message']
    
    def validate_description(self, description: str) -> bool:
        """Validate AI-generated description against quality criteria"""
        if not self.validate_responses:
            return True
        
        # Length validation
        if len(description) < self.min_description_length:
            return False
        
        if len(description) > self.max_description_length:
            return False
        
        # Content validation
        if self.require_complete_descriptions and not description.strip():
            return False
        
        return True
    
    def truncate_sample_values(self, sample_values: str) -> str:
        """Truncate sample values to configured length"""
        if len(sample_values) <= self.max_sample_values_length:
            return sample_values
        
        return sample_values[:self.max_sample_values_length] + "..."
    
    def get_effective_token_limit(self, content_type: str) -> int:
        """Get effective token limit accounting for system message buffer"""
        base_limits = {
            'table': self.table_description_max_tokens,
            'column': self.column_description_max_tokens,
            'batch': self.batch_description_max_tokens
        }
        
        base_limit = base_limits.get(content_type, self.column_description_max_tokens)
        return max(100, base_limit - self._config['tokens']['system_message_buffer'])

# Global instance
AI_CONFIG = AIConfig()