"""AI-powered documentation generation agent"""

import pandas as pd
from typing import List, Dict, Any
from crewai import Agent, Task, Crew
import anthropic
from src.core.config import APP_CONFIG, logger

class DocumentationAgent:
    """CrewAI agent for generating business documentation"""
    
    def __init__(self):
        self.anthropic_client = anthropic.Anthropic(api_key=APP_CONFIG.anthropic_api_key)
        self.agent = self._create_agent()
    
    def _create_agent(self) -> Agent:
        """Create the documentation agent"""
        return Agent(
            role="Business Documentation Specialist",
            goal="Transform technical metadata into clear business documentation",
            backstory="""You are an expert at translating technical database schemas into 
            business-friendly documentation. You understand both data architecture and 
            business needs, creating descriptions that help users discover and understand data.""",
            verbose=True,
            allow_delegation=False
        )
    
    def generate_documentation(self, enriched_df: pd.DataFrame) -> pd.DataFrame:
        """Generate business documentation for all database objects"""
        logger.info(f"Generating documentation for {len(enriched_df)} columns")
        
        # Group by table for efficient processing
        documented_data = []
        
        for table_name in enriched_df['table_name'].unique():
            table_df = enriched_df[enriched_df['table_name'] == table_name]
            
            try:
                # Generate table description
                table_description = self._generate_table_description(table_name, table_df)
                
                # Generate column descriptions in batches
                columns_with_descriptions = self._generate_column_descriptions(table_df, table_description)
                
                documented_data.extend(columns_with_descriptions)
                
            except Exception as e:
                logger.error(f"Failed to document table {table_name}: {str(e)}")
                # Add without descriptions
                for _, row in table_df.iterrows():
                    documented_data.append(dict(row))
        
        return pd.DataFrame(documented_data)
    
    def _generate_table_description(self, table_name: str, table_df: pd.DataFrame) -> str:
        """Generate business description for a table"""
        logger.info(f"Generating table description for {table_name}")
        
        # Analyze table structure
        column_info = []
        for _, col in table_df.head(10).iterrows():  # Top 10 columns for context
            info = f"- {col['column_name']} ({col['business_data_type']})"
            if col.get('sample_values'):
                info += f": {str(col['sample_values'])[:50]}..."
            column_info.append(info)
        
        prompt = f"""Provide a clear business description for the database table '{table_name}'.

Table has {len(table_df)} columns including:
{chr(10).join(column_info)}

Provide a 2-3 sentence business description explaining:
1. What this table contains
2. Its business purpose
3. How it might be used

Keep it business-focused and accessible to non-technical users."""
        
        try:
            response = self.anthropic_client.messages.create(
                model=APP_CONFIG.model_name,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Failed to generate table description: {str(e)}")
            return f"Business data table containing {len(table_df)} data elements."
    
    def _generate_column_descriptions(self, table_df: pd.DataFrame, table_description: str) -> List[Dict]:
        """Generate descriptions for table columns"""
        columns_data = []
        
        # Process in batches
        for i in range(0, len(table_df), APP_CONFIG.batch_size):
            batch = table_df.iloc[i:i+APP_CONFIG.batch_size]
            
            try:
                batch_descriptions = self._generate_batch_descriptions(batch, table_description)
                
                for j, (_, row) in enumerate(batch.iterrows()):
                    column_data = dict(row)
                    column_data['table_description'] = table_description
                    
                    if j < len(batch_descriptions):
                        column_data['column_description'] = batch_descriptions[j]
                    else:
                        column_data['column_description'] = ""
                    
                    columns_data.append(column_data)
                    
            except Exception as e:
                logger.error(f"Failed to generate batch descriptions: {str(e)}")
                # Add without descriptions
                for _, row in batch.iterrows():
                    column_data = dict(row)
                    column_data['table_description'] = table_description
                    column_data['column_description'] = ""
                    columns_data.append(column_data)
        
        return columns_data
    
    def _generate_batch_descriptions(self, batch_df: pd.DataFrame, table_context: str) -> List[str]:
        """Generate descriptions for a batch of columns"""
        column_info = []
        for _, col in batch_df.iterrows():
            info = f"{col['column_name']} ({col['data_type']}, {col['business_data_type']})"
            if col.get('sample_values'):
                info += f" - samples: {str(col['sample_values'])[:30]}..."
            column_info.append(info)
        
        prompt = f"""Provide concise business descriptions for these database columns.

Table context: {table_context}

Columns:
{chr(10).join(column_info)}

For each column, provide a 1-sentence business description explaining what it contains and how it's used.
Return only the descriptions, one per line, in the same order as the columns listed above."""
        
        try:
            response = self.anthropic_client.messages.create(
                model=APP_CONFIG.model_name,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            descriptions = response.content[0].text.strip().split('\n')
            # Clean up descriptions
            cleaned_descriptions = []
            for desc in descriptions:
                desc = desc.strip()
                if desc.startswith(('-', '•', '*')):
                    desc = desc[1:].strip()
                if desc:
                    cleaned_descriptions.append(desc)
            
            return cleaned_descriptions
            
        except Exception as e:
            logger.error(f"Failed to generate batch descriptions: {str(e)}")
            return ["" for _ in range(len(batch_df))]
