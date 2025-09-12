"""AI-powered documentation generation agent - Multi-provider support"""

import pandas as pd
import time
import os
from typing import List, Dict, Any, Optional
from crewai import Agent, Task, Crew
from src.core.config import APP_CONFIG, logger
from src.core.ai_config import AI_CONFIG

class MultiProviderAIClient:
    """Unified client for multiple AI providers"""
    
    def __init__(self):
        self.provider = AI_CONFIG.provider
        self.client = self._initialize_client()
        logger.info(f"Initialized AI client for provider: {self.provider}")
    
    def _initialize_client(self):
        """Initialize the appropriate AI client based on provider"""
        try:
            if self.provider == "anthropic":
                import anthropic
                api_key = AI_CONFIG.get_api_key_for_provider("anthropic")
                if not api_key:
                    api_key = APP_CONFIG.anthropic_api_key  # Fallback to existing config
                return anthropic.Anthropic(api_key=api_key)
            
            elif self.provider == "openai":
                import openai
                api_key = AI_CONFIG.get_api_key_for_provider("openai")
                org_id = os.getenv(AI_CONFIG.openai_config.get('organization_env', ''))
                client_kwargs = {"api_key": api_key}
                if org_id:
                    client_kwargs["organization"] = org_id
                return openai.OpenAI(**client_kwargs)
            
            elif self.provider == "gemini":
                import google.generativeai as genai
                api_key = AI_CONFIG.get_api_key_for_provider("gemini")
                genai.configure(api_key=api_key)
                return genai
            
            elif self.provider == "azure":
                import openai
                api_key = AI_CONFIG.get_api_key_for_provider("azure")
                endpoint = os.getenv(AI_CONFIG.azure_config.get('endpoint_env', ''))
                api_version = AI_CONFIG.azure_config.get('api_version', '2024-02-15-preview')
                return openai.AzureOpenAI(
                    api_key=api_key,
                    azure_endpoint=endpoint,
                    api_version=api_version
                )
            
            elif self.provider == "litellm":
                import litellm
                return litellm
            
            else:
                # Fallback to Anthropic for unknown providers
                logger.warning(f"Unknown provider '{self.provider}', falling back to Anthropic")
                import anthropic
                return anthropic.Anthropic(api_key=APP_CONFIG.anthropic_api_key)
                
        except ImportError as e:
            logger.error(f"Failed to import required library for {self.provider}: {e}")
            logger.info("Falling back to Anthropic client")
            import anthropic
            return anthropic.Anthropic(api_key=APP_CONFIG.anthropic_api_key)
    
    def generate_text(self, prompt: str, max_tokens: int = 1000, model: str = None) -> str:
        """Generate text using the configured provider"""
        if model is None:
            model = AI_CONFIG.get_model_for_task()
        
        try:
            if self.provider == "anthropic":
                response = self.client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
            
            elif self.provider in ["openai", "azure"]:
                response = self.client.chat.completions.create(
                    model=model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.choices[0].message.content
            
            elif self.provider == "gemini":
                model_instance = self.client.GenerativeModel(model)
                generation_config = {
                    'max_output_tokens': max_tokens,
                    'temperature': 0.7,
                }
                response = model_instance.generate_content(
                    prompt,
                    generation_config=generation_config
                )
                return response.text
            
            elif self.provider == "litellm":
                response = self.client.completion(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
                
        except Exception as e:
            logger.error(f"Error generating text with {self.provider}: {str(e)}")
            raise

class DocumentationAgent:
    """CrewAI agent for generating business documentation - Multi-provider support"""
    
    def __init__(self):
        self.ai_client = MultiProviderAIClient()
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the documentation agent - UPDATED with configurable system message"""
        return Agent(
            role="Business Documentation Specialist",
            goal="Transform technical metadata into clear business documentation",
            backstory=AI_CONFIG.get_system_message(),
            verbose=True,
            allow_delegation=False
        )
    
    def generate_documentation(self, enriched_df: pd.DataFrame) -> pd.DataFrame:
        """Generate business documentation for all database objects"""
        logger.info(f"Generating documentation for {len(enriched_df)} columns using {AI_CONFIG.primary_model} ({AI_CONFIG.provider})")
        
        # Group by table for efficient processing
        documented_data = []
        
        for table_name in enriched_df['table_name'].unique():
            table_df = enriched_df[enriched_df['table_name'] == table_name]
            
            try:
                # Generate table description using configurable prompts
                table_description = self._generate_table_description(table_name, table_df)
                
                # Generate column descriptions in configurable batches
                columns_with_descriptions = self._generate_column_descriptions(table_df, table_description)
                
                documented_data.extend(columns_with_descriptions)
                
            except Exception as e:
                logger.error(f"Failed to document table {table_name}: {str(e)}")
                # Add without descriptions
                for _, row in table_df.iterrows():
                    documented_data.append(dict(row))
        
        return pd.DataFrame(documented_data)
    
    def _generate_table_description(self, table_name: str, table_df: pd.DataFrame) -> str:
        """Generate business description for a table - UPDATED with configurable prompts"""
        logger.info(f"Generating table description for {table_name}")
        
        # Analyze table structure using configurable context limits
        column_info = []
        max_columns = AI_CONFIG.max_context_columns
        
        for _, col in table_df.head(max_columns).iterrows():
            info = f"- {col['column_name']} ({col['business_data_type']})"
            if col.get('sample_values'):
                # Truncate sample values using config
                sample_values = AI_CONFIG.truncate_sample_values(str(col['sample_values']))
                info += f": {sample_values}"
            column_info.append(info)
        
        # Generate prompt using configurable template
        prompt = AI_CONFIG.get_table_description_prompt(
            table_name=table_name,
            column_count=len(table_df),
            column_info='\n'.join(column_info)
        )
        
        try:
            # Select model based on configuration
            model = AI_CONFIG.get_model_for_task("standard")
            max_tokens = AI_CONFIG.get_effective_token_limit("table")
            
            response_text = self._make_api_request(
                prompt=prompt,
                model=model,
                max_tokens=max_tokens
            )
            
            description = response_text.strip()
            
            # Validate response using config criteria
            if AI_CONFIG.validate_description(description):
                return description
            else:
                logger.warning(f"Generated table description for {table_name} failed validation")
                return f"Business data table containing {len(table_df)} data elements."
                
        except Exception as e:
            logger.error(f"Failed to generate table description: {str(e)}")
            return f"Business data table containing {len(table_df)} data elements."
    
    def _generate_column_descriptions(self, table_df: pd.DataFrame, table_description: str) -> List[Dict]:
        """Generate descriptions for table columns - UPDATED with configurable batching"""
        columns_data = []
        
        # Use configurable batch size
        batch_size = AI_CONFIG.batch_size
        
        # Process in batches
        for i in range(0, len(table_df), batch_size):
            batch = table_df.iloc[i:i+batch_size]
            
            try:
                batch_descriptions = self._generate_batch_descriptions(batch, table_description)
                
                for j, (_, row) in enumerate(batch.iterrows()):
                    column_data = dict(row)
                    column_data['table_description'] = table_description
                    
                    if j < len(batch_descriptions):
                        description = batch_descriptions[j]
                        # Validate individual column description
                        if AI_CONFIG.validate_description(description):
                            column_data['column_description'] = description
                        else:
                            column_data['column_description'] = f"Data field: {row['column_name']}"
                    else:
                        column_data['column_description'] = f"Data field: {row['column_name']}"
                    
                    columns_data.append(column_data)
                    
            except Exception as e:
                logger.error(f"Failed to generate batch descriptions: {str(e)}")
                # Add without descriptions
                for _, row in batch.iterrows():
                    column_data = dict(row)
                    column_data['table_description'] = table_description
                    column_data['column_description'] = f"Data field: {row['column_name']}"
                    columns_data.append(column_data)
        
        return columns_data
    
    def _generate_batch_descriptions(self, batch_df: pd.DataFrame, table_context: str) -> List[str]:
        """Generate descriptions for a batch of columns - UPDATED with configurable prompts"""
        column_info = []
        for _, col in batch_df.iterrows():
            info = f"{col['column_name']} ({col['data_type']}, {col['business_data_type']})"
            if col.get('sample_values'):
                # Truncate sample values using config
                sample_values = AI_CONFIG.truncate_sample_values(str(col['sample_values']))
                info += f" - samples: {sample_values}"
            column_info.append(info)
        
        # Generate prompt using configurable template
        prompt = AI_CONFIG.get_column_description_prompt(
            table_context=table_context,
            column_info='\n'.join(column_info)
        )
        
        try:
            # Select model and token limit based on batch complexity
            complexity = "complex" if len(batch_df) > 5 else "standard"
            model = AI_CONFIG.get_model_for_task(complexity)
            max_tokens = AI_CONFIG.get_effective_token_limit("batch")
            
            response_text = self._make_api_request(
                prompt=prompt,
                model=model,
                max_tokens=max_tokens
            )
            
            descriptions = response_text.strip().split('\n')
            
            # Clean up descriptions
            cleaned_descriptions = []
            for desc in descriptions:
                desc = desc.strip()
                if desc.startswith(('-', '•', '*')):
                    desc = desc[1:].strip()
                if desc:
                    # Truncate if too long
                    if len(desc) > AI_CONFIG.max_description_length:
                        desc = desc[:AI_CONFIG.max_description_length] + "..."
                    cleaned_descriptions.append(desc)
            
            return cleaned_descriptions
            
        except Exception as e:
            logger.error(f"Failed to generate batch descriptions: {str(e)}")
            return [f"Data field: {row['column_name']}" for _, row in batch_df.iterrows()]
    
    def _make_api_request(self, prompt: str, model: str = None, max_tokens: int = None) -> str:
        """Make API request with configurable retry logic - UPDATED for multi-provider"""
        if model is None:
            model = AI_CONFIG.primary_model
        if max_tokens is None:
            max_tokens = AI_CONFIG.column_description_max_tokens
        
        last_exception = None
        current_model = model
        
        for attempt in range(AI_CONFIG.max_retries):
            try:
                # Use the unified AI client
                response_text = self.ai_client.generate_text(
                    prompt=prompt,
                    max_tokens=max_tokens,
                    model=current_model
                )
                return response_text
                
            except Exception as e:
                last_exception = e
                logger.warning(f"API request attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < AI_CONFIG.max_retries - 1:
                    # Try fallback model on subsequent attempts
                    if current_model == AI_CONFIG.primary_model and attempt > 0:
                        current_model = AI_CONFIG.fallback_model
                        logger.info(f"Switching to fallback model: {current_model}")
                    
                    # Wait before retry
                    time.sleep(AI_CONFIG.retry_delay_seconds)
                else:
                    logger.error(f"All {AI_CONFIG.max_retries} API request attempts failed")
        
        # If all retries failed, raise the last exception
        raise last_exception