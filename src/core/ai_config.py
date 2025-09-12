"""AI configuration loader - Refactored to use BaseConfig"""
import os
from typing import Dict, Any
from src.core.base_config import BaseConfig

class AIConfig(BaseConfig):
    """Loads and manages AI configuration using BaseConfig"""
    
    def __init__(self, config_path: str = "config/ai_settings.yaml"):
        super().__init__(config_path, "AI settings")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults"""
        return {
            'models': {
                'provider': 'anthropic',
                'primary_model': 'claude-3-haiku-20240307',
                'fallback_model': 'claude-3-sonnet-20240229',
                'fast_model': 'claude-3-haiku-20240307',
                'detailed_model': 'claude-3-sonnet-20240229',
                'auto_select_model': False,
                'anthropic': {
                    'primary_model': 'claude-3-haiku-20240307',
                    'fallback_model': 'claude-3-sonnet-20240229',
                    'api_key_env': 'ANTHROPIC_API_KEY'
                },
                'openai': {
                    'primary_model': 'gpt-4o-mini',
                    'fallback_model': 'gpt-3.5-turbo',
                    'api_key_env': 'OPENAI_API_KEY'
                }
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
    
    # Model configuration properties (using dot notation now!)
    @property
    def primary_model(self) -> str:
        return self.get('models.primary_model', 'claude-3-haiku-20240307')
    
    @property
    def fallback_model(self) -> str:
        return self.get('models.fallback_model', 'claude-3-sonnet-20240229')
    
    @property
    def provider(self) -> str:
        return self.get('models.provider', 'anthropic')
    
    @property
    def fast_model(self) -> str:
        return self.get('models.fast_model', 'claude-3-haiku-20240307')
    
    @property
    def detailed_model(self) -> str:
        return self.get('models.detailed_model', 'claude-3-sonnet-20240229')
    
    @property
    def auto_select_model(self) -> bool:
        return self.get('models.auto_select_model', False)
    
    # Token configuration properties
    @property
    def table_description_max_tokens(self) -> int:
        return self.get('tokens.table_description_max', 500)
    
    @property
    def column_description_max_tokens(self) -> int:
        return self.get('tokens.column_description_max', 1000)
    
    @property
    def batch_description_max_tokens(self) -> int:
        return self.get('tokens.batch_description_max', 1500)
    
    @property
    def max_context_columns(self) -> int:
        return self.get('tokens.max_context_columns', 10)
    
    @property
    def max_sample_values_length(self) -> int:
        return self.get('tokens.max_sample_values_length', 100)
    
    # Performance configuration properties
    @property
    def batch_size(self) -> int:
        return self.get('performance.batch_size', 10)
    
    @property
    def max_retries(self) -> int:
        return self.get('performance.max_retries', 3)
    
    @property
    def retry_delay_seconds(self) -> int:
        return self.get('performance.retry_delay_seconds', 2)
    
    @property
    def request_timeout_seconds(self) -> int:
        return self.get('performance.request_timeout_seconds', 60)
    
    # Quality configuration properties
    @property
    def require_complete_descriptions(self) -> bool:
        return self.get('quality.require_complete_descriptions', True)
    
    @property
    def min_description_length(self) -> int:
        return self.get('quality.min_description_length', 20)
    
    @property
    def max_description_length(self) -> int:
        return self.get('quality.max_description_length', 200)
    
    @property
    def validate_responses(self) -> bool:
        return self.get('quality.validate_responses', True)
    
    # Customization properties
    @property
    def business_domain(self) -> str:
        return self.get('customization.business_domain', 'general')
    
    @property
    def tone(self) -> str:
        return self.get('customization.tone', 'professional')
    
    @property
    def audience(self) -> str:
        return self.get('customization.audience', 'business_users')
    
    # Provider-specific methods
    @property
    def openai_config(self) -> Dict[str, Any]:
        return self.get('models.openai', {})

    @property
    def gemini_config(self) -> Dict[str, Any]:
        return self.get('models.gemini', {})

    @property
    def azure_config(self) -> Dict[str, Any]:
        return self.get('models.azure', {})

    def get_api_key_for_provider(self, provider: str = None) -> str:
        """Get API key for specified provider"""
        if provider is None:
            provider = self.provider
        
        config = self.get(f'models.{provider}', {})
        api_key_env = config.get('api_key_env')
        
        if api_key_env:
            return os.getenv(api_key_env)
        return None
    
    # Helper methods (unchanged)
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
        template = self.get('prompts.table_description_template', '')
        
        # Check for domain-specific templates
        domain_template_key = f"prompts.{self.business_domain}_table_template"
        domain_template = self.get(domain_template_key)
        if domain_template:
            template = domain_template
        
        return template.format(
            table_name=table_name,
            column_count=column_count,
            column_info=column_info
        )
    
    def get_column_description_prompt(self, table_context: str, column_info: str) -> str:
        """Generate column description prompt using template"""
        template = self.get('prompts.column_description_template', '')
        return template.format(
            table_context=table_context,
            column_info=column_info
        )
    
    def get_system_message(self) -> str:
        """Get system message for the AI agent"""
        return self.get('prompts.system_message', '')
    
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
        max_length = self.max_sample_values_length
        if len(sample_values) <= max_length:
            return sample_values
        
        return sample_values[:max_length] + "..."
    
    def get_effective_token_limit(self, content_type: str) -> int:
        """Get effective token limit accounting for system message buffer"""
        base_limits = {
            'table': self.table_description_max_tokens,
            'column': self.column_description_max_tokens,
            'batch': self.batch_description_max_tokens
        }
        
        base_limit = base_limits.get(content_type, self.column_description_max_tokens)
        buffer = self.get('tokens.system_message_buffer', 100)
        return max(100, base_limit - buffer)

# Global instance
AI_CONFIG = AIConfig()