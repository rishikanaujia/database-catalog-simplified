#!/bin/bash

# Database Catalog Generator Setup Script
# Creates simplified architecture with tools + single CrewAI agent

set -e

PROJECT_NAME="database-catalog-simplified"
PROJECT_DIR=$(pwd)/$PROJECT_NAME

echo "Creating Database Catalog project structure..."

# Create project directory
mkdir -p $PROJECT_DIR
cd $PROJECT_DIR

# Create directory structure
mkdir -p {src/{tools,agents,core},outputs,logs,tests}

echo "📁 Created directory structure"

# Create requirements.txt
cat > requirements.txt << 'EOF'
# Core dependencies
crewai>=0.177.0
anthropic>=0.34.0
openai>=1.50.0

# Database connectivity
snowflake-connector-python==3.17.3
python-dotenv==1.0.1

# Data processing
pandas==2.3.2
numpy==2.3.2

# UI framework
gradio>=5.0.0

# Utilities
pydantic>=2.9.0
jsonlines==4.0.0

# Development
pytest>=8.3.0
black>=24.8.0
EOF

# Create .env.template
cat > .env.template << 'EOF'
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Optional Snowflake settings
SNOWFLAKE_REGION=us-west-2
SNOWFLAKE_AUTHENTICATOR=snowflake

# AI Configuration
ANTHROPIC_API_KEY=your_anthropic_api_key
MODEL_NAME=claude-3-haiku-20240307

# Application Settings
LOG_LEVEL=INFO
SAMPLE_SIZE=1000
BATCH_SIZE=10
OUTPUT_DIR=./outputs
EOF

# Create configuration module
cat > src/core/config.py << 'EOF'
"""Configuration management for Database Catalog"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, field_validator

load_dotenv()

class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema_name: str = ""
    role: Optional[str] = None
    region: Optional[str] = None
    authenticator: str = "snowflake"

    def __init__(self, **kwargs):
        super().__init__(
            account=kwargs.get('account', os.getenv("SNOWFLAKE_ACCOUNT", "")),
            user=kwargs.get('user', os.getenv("SNOWFLAKE_USER", "")),
            password=kwargs.get('password', os.getenv("SNOWFLAKE_PASSWORD", "")),
            warehouse=kwargs.get('warehouse', os.getenv("SNOWFLAKE_WAREHOUSE", "")),
            database=kwargs.get('database', os.getenv("SNOWFLAKE_DATABASE", "")),
            schema_name=kwargs.get('schema_name', os.getenv("SNOWFLAKE_SCHEMA", "")),
            role=kwargs.get('role', os.getenv("SNOWFLAKE_ROLE")),
            region=kwargs.get('region', os.getenv("SNOWFLAKE_REGION")),
            authenticator=kwargs.get('authenticator', os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake"))
        )

    @field_validator('account', 'user', 'password', 'warehouse', 'database', 'schema_name')
    @classmethod
    def validate_required_fields(cls, v: str, info):
        if not v:
            raise ValueError(f"{info.field_name} is required")
        return v

    def get_connection_params(self):
        """Return connection parameters for snowflake.connector"""
        params = {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema_name,
            'authenticator': self.authenticator
        }
        if self.role:
            params['role'] = self.role
        if self.region:
            params['region'] = self.region
        return params

class AppConfig(BaseModel):
    """Application configuration"""
    log_level: str = "INFO"
    sample_size: int = 1000
    batch_size: int = 10
    output_dir: str = "./outputs"
    anthropic_api_key: Optional[str] = None
    model_name: str = "claude-3-haiku-20240307"

    def __init__(self, **kwargs):
        super().__init__(
            log_level=kwargs.get('log_level', os.getenv("LOG_LEVEL", "INFO")),
            sample_size=kwargs.get('sample_size', int(os.getenv("SAMPLE_SIZE", "1000"))),
            batch_size=kwargs.get('batch_size', int(os.getenv("BATCH_SIZE", "10"))),
            output_dir=kwargs.get('output_dir', os.getenv("OUTPUT_DIR", "./outputs")),
            anthropic_api_key=kwargs.get('anthropic_api_key', os.getenv("ANTHROPIC_API_KEY")),
            model_name=kwargs.get('model_name', os.getenv("MODEL_NAME", "claude-3-haiku-20240307"))
        )
        
        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def get_timestamped_filename(self, base_name: str, extension: str = "csv") -> str:
        """Generate timestamped filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{timestamp}.{extension}"

    def get_output_path(self, filename: str) -> Path:
        """Return full path for an output file"""
        return Path(self.output_dir) / filename

# Global config instances
DB_CONFIG = DatabaseConfig()
APP_CONFIG = AppConfig()

# Setup logging
def setup_logging():
    logging.basicConfig(
        level=getattr(logging, APP_CONFIG.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(APP_CONFIG.get_output_path("catalog.log"))
        ]
    )
    return logging.getLogger("database_catalog")

logger = setup_logging()
EOF

# Create database connector tool
cat > src/tools/database_connector.py << 'EOF'
"""Database connection utility with retry logic"""

import time
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Optional, Dict, Any
from src.core.config import DB_CONFIG, logger

class DatabaseConnector:
    """Handles database connections with retry logic"""
    
    def __init__(self):
        self.connection = None
        self.max_retries = 3
    
    def connect(self) -> bool:
        """Establish database connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connection attempt {attempt + 1}/{self.max_retries}")
                
                conn_params = DB_CONFIG.get_connection_params()
                conn_params.update({
                    'network_timeout': 30,
                    'client_session_keep_alive': True,
                    'autocommit': True
                })
                
                self.connection = snowflake.connector.connect(**conn_params)
                
                # Test connection
                cursor = self.connection.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                cursor.fetchone()
                cursor.close()
                
                logger.info("Database connection established successfully")
                return True
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect after {self.max_retries} attempts")
                    return False
    
    def execute_query(self, query: str, params: Optional[list] = None):
        """Execute query and return results"""
        if not self.connection:
            raise ConnectionError("Database not connected")
        
        cursor = self.connection.cursor(DictCursor)
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
EOF

# Create schema discovery tool
cat > src/tools/schema_discoverer.py << 'EOF'
"""Schema discovery and metadata collection tool"""

import pandas as pd
from typing import List, Dict, Any
from src.core.config import DB_CONFIG, logger
from src.tools.database_connector import DatabaseConnector

class SchemaDiscoverer:
    """Discovers database schema and collects metadata"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    def discover_tables(self) -> pd.DataFrame:
        """Discover all tables in the schema"""
        logger.info(f"Discovering tables in {DB_CONFIG.database}.{DB_CONFIG.schema_name}")
        
        query = f"""
        SHOW TABLES IN SCHEMA {DB_CONFIG.database}.{DB_CONFIG.schema_name}
        """
        
        tables_data = self.db.execute_query(query)
        
        # Convert to DataFrame
        if tables_data:
            df = pd.DataFrame(tables_data)
            logger.info(f"Found {len(df)} tables")
            return df
        else:
            return pd.DataFrame()
    
    def get_table_metadata(self, table_names: List[str]) -> pd.DataFrame:
        """Get detailed metadata for tables"""
        logger.info(f"Collecting metadata for {len(table_names)} tables")
        
        placeholders = ','.join(['%s'] * len(table_names))
        query = f"""
        SELECT 
            table_name,
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            comment
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name IN ({placeholders})
        ORDER BY table_name, ordinal_position
        """
        
        params = [DB_CONFIG.schema_name] + table_names
        columns_data = self.db.execute_query(query, params)
        
        df = pd.DataFrame(columns_data)
        logger.info(f"Collected metadata for {len(df)} columns")
        return df
    
    def analyze_column_roles(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze and classify column roles"""
        logger.info("Analyzing column roles and business types")
        
        def classify_column(row):
            col_name = row['column_name'].lower()
            data_type = row['data_type'].upper()
            
            # Role classification
            if col_name.endswith('_sk') and row['is_nullable'] == 'NO':
                return 'primary_key', 'Identifier'
            elif col_name.endswith('_sk'):
                return 'foreign_key', 'Identifier'
            elif any(word in col_name for word in ['amount', 'price', 'cost']):
                return 'measure', 'Currency'
            elif any(word in col_name for word in ['quantity', 'qty', 'count']):
                return 'measure', 'Quantity'
            elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                return 'dimension', 'Date'
            elif any(word in col_name for word in ['name', 'desc', 'description']):
                return 'dimension', 'Description'
            else:
                return 'dimension', 'Text' if data_type in ['TEXT', 'VARCHAR'] else 'Numeric'
        
        # Apply classification
        roles_and_types = metadata_df.apply(classify_column, axis=1, result_type='expand')
        metadata_df['column_role'] = roles_and_types[0]
        metadata_df['business_data_type'] = roles_and_types[1]
        
        return metadata_df
EOF

# Create data profiler tool
cat > src/tools/data_profiler.py << 'EOF'
"""Data profiling tool for sampling and statistics"""

import pandas as pd
from typing import Dict, Any, List
from src.core.config import DB_CONFIG, APP_CONFIG, logger
from src.tools.database_connector import DatabaseConnector

class DataProfiler:
    """Profiles data and collects samples/statistics"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    def profile_columns(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Profile columns with samples and statistics"""
        logger.info(f"Profiling {len(metadata_df)} columns")
        
        enriched_data = []
        
        for _, row in metadata_df.iterrows():
            table_name = row['table_name']
            column_name = row['column_name']
            data_type = row['data_type']
            
            try:
                profile_data = self._profile_single_column(table_name, column_name, data_type)
                
                # Merge with existing metadata
                enriched_row = dict(row)
                enriched_row.update(profile_data)
                enriched_data.append(enriched_row)
                
            except Exception as e:
                logger.warning(f"Failed to profile {table_name}.{column_name}: {str(e)}")
                enriched_data.append(dict(row))
        
        return pd.DataFrame(enriched_data)
    
    def _profile_single_column(self, table_name: str, column_name: str, data_type: str) -> Dict[str, Any]:
        """Profile a single column"""
        profile = {}
        
        # Determine sampling strategy
        row_count = self._get_table_row_count(table_name)
        sample_clause = self._get_sample_clause(row_count)
        
        try:
            if data_type.upper() in ['NUMBER', 'INTEGER', 'BIGINT', 'DECIMAL']:
                profile.update(self._profile_numeric_column(table_name, column_name, sample_clause))
            elif data_type.upper() in ['TEXT', 'VARCHAR', 'CHAR']:
                profile.update(self._profile_text_column(table_name, column_name, sample_clause))
            elif data_type.upper() in ['DATE', 'DATETIME', 'TIMESTAMP']:
                profile.update(self._profile_date_column(table_name, column_name, sample_clause))
        except Exception as e:
            profile['profiling_error'] = str(e)
        
        return profile
    
    def _get_table_row_count(self, table_name: str) -> int:
        """Get approximate row count for table"""
        try:
            query = f"SELECT COUNT(*) as cnt FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}"
            result = self.db.execute_query(query)
            return result[0]['CNT'] if result else 0
        except:
            return 1000000  # Default assumption
    
    def _get_sample_clause(self, row_count: int) -> str:
        """Determine sampling strategy based on table size"""
        if row_count < 10000:
            return ""
        elif row_count < 1000000:
            return "TABLESAMPLE BERNOULLI(10)"
        else:
            return f"TABLESAMPLE ({APP_CONFIG.sample_size} ROWS)"
    
    def _profile_numeric_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile numeric column"""
        query = f"""
        SELECT 
            MIN({column_name}) as min_value,
            MAX({column_name}) as max_value,
            AVG({column_name}) as avg_value,
            COUNT(DISTINCT {column_name}) as distinct_count
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        """
        
        result = self.db.execute_query(query)
        if result:
            return dict(result[0])
        return {}
    
    def _profile_text_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile text column"""
        query = f"""
        SELECT DISTINCT {column_name}
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        ORDER BY {column_name}
        LIMIT 5
        """
        
        result = self.db.execute_query(query)
        if result:
            sample_values = [row[column_name.upper()] for row in result]
            return {'sample_values': '; '.join(map(str, sample_values))}
        return {}
    
    def _profile_date_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile date column"""
        query = f"""
        SELECT 
            MIN({column_name}) as min_value,
            MAX({column_name}) as max_value,
            COUNT(DISTINCT {column_name}) as distinct_count
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        """
        
        result = self.db.execute_query(query)
        if result:
            return {
                'min_value': str(result[0]['MIN_VALUE']) if result[0]['MIN_VALUE'] else None,
                'max_value': str(result[0]['MAX_VALUE']) if result[0]['MAX_VALUE'] else None,
                'distinct_count': result[0]['DISTINCT_COUNT']
            }
        return {}
EOF

# Create documentation agent (the only true CrewAI agent)
cat > src/agents/documentation_agent.py << 'EOF'
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
EOF

# Create UI tool
cat > src/tools/ui_generator.py << 'EOF'
"""UI generation tool for data catalog interface"""

import pandas as pd
import gradio as gr
from typing import Dict, Any, Tuple
from src.core.config import logger

class UIGenerator:
    """Generates interactive web interface for data catalog"""
    
    def __init__(self, documented_df: pd.DataFrame):
        self.df = documented_df
        self.tables = sorted(self.df['table_name'].unique())
        self.business_types = sorted(self.df['business_data_type'].unique())
    
    def create_interface(self) -> str:
        """Create and launch Gradio interface"""
        logger.info("Creating Gradio interface for data catalog")
        
        def search_catalog(search_term: str, table_filter: str, type_filter: str) -> Tuple[str, str]:
            """Search the data catalog"""
            filtered_df = self.df.copy()
            
            # Apply filters
            if table_filter != "All Tables":
                filtered_df = filtered_df[filtered_df['table_name'] == table_filter]
            
            if type_filter != "All Types":
                filtered_df = filtered_df[filtered_df['business_data_type'] == type_filter]
            
            # Apply search
            if search_term:
                search_mask = (
                    filtered_df['table_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['column_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['column_description'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[search_mask]
            
            if len(filtered_df) == 0:
                return "No results found.", ""
            
            # Create results HTML
            results_html = self._format_results(filtered_df.head(50))
            summary = f"Found {len(filtered_df)} results across {filtered_df['table_name'].nunique()} tables"
            
            return summary, results_html
        
        def get_table_details(table_name: str) -> str:
            """Get detailed table information"""
            if table_name == "Select a table":
                return "Please select a table to view details."
            
            table_df = self.df[self.df['table_name'] == table_name]
            if len(table_df) == 0:
                return "Table not found."
            
            # Get table description
            table_desc = table_df.iloc[0].get('table_description', 'No description available.')
            
            # Create table details HTML
            details_html = f"""
            <h2>{table_name}</h2>
            <p><strong>Description:</strong> {table_desc}</p>
            <p><strong>Columns:</strong> {len(table_df)}</p>
            
            <table style="width:100%; border-collapse: collapse;">
                <tr style="background-color: #f2f2f2;">
                    <th style="border: 1px solid #ddd; padding: 8px;">Column</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Type</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Role</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Description</th>
                </tr>
            """
            
            for _, col in table_df.iterrows():
                details_html += f"""
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px;"><code>{col['column_name']}</code></td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col['data_type']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col['column_role']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col.get('column_description', '')}</td>
                </tr>
                """
            
            details_html += "</table>"
            return details_html
        
        # Create Gradio interface
        with gr.Blocks(title="Database Catalog", theme=gr.themes.Soft()) as interface:
            gr.Markdown(f"""
            # Database Catalog
            Interactive catalog for {len(self.tables)} tables and {len(self.df)} columns
            """)
            
            with gr.Tabs():
                with gr.Tab("Search Catalog"):
                    with gr.Row():
                        search_input = gr.Textbox(label="Search", placeholder="Search tables, columns, or descriptions...")
                        table_filter = gr.Dropdown(choices=["All Tables"] + self.tables, label="Table", value="All Tables")
                        type_filter = gr.Dropdown(choices=["All Types"] + self.business_types, label="Business Type", value="All Types")
                    
                    search_btn = gr.Button("Search", variant="primary")
                    search_summary = gr.Markdown()
                    search_results = gr.HTML()
                    
                    search_btn.click(
                        search_catalog,
                        inputs=[search_input, table_filter, type_filter],
                        outputs=[search_summary, search_results]
                    )
                
                with gr.Tab("Browse Tables"):
                    table_selector = gr.Dropdown(choices=["Select a table"] + self.tables, label="Choose Table", value="Select a table")
                    table_details = gr.HTML()
                    
                    table_selector.change(
                        get_table_details,
                        inputs=[table_selector],
                        outputs=[table_details]
                    )
        
        # Launch interface
        url = interface.launch(server_name="0.0.0.0", server_port=7860, share=False, inbrowser=True)
        return "http://localhost:7860"
    
    def _format_results(self, df: pd.DataFrame) -> str:
        """Format search results as HTML table"""
        html = """
        <table style="width:100%; border-collapse: collapse;">
            <tr style="background-color: #f2f2f2;">
                <th style="border: 1px solid #ddd; padding: 8px;">Table</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Column</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Type</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Business Type</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Description</th>
            </tr>
        """
        
        for _, row in df.iterrows():
            description = str(row.get('column_description', ''))[:100] + "..." if len(str(row.get('column_description', ''))) > 100 else str(row.get('column_description', ''))
            
            html += f"""
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>{row['table_name']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;"><code>{row['column_name']}</code></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{row['data_type']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{row['business_data_type']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{description}</td>
            </tr>
            """
        
        html += "</table>"
        return html
EOF

# Create pipeline orchestrator
cat > src/core/pipeline.py << 'EOF'
"""Main pipeline orchestrator"""

import pandas as pd
from src.core.config import DB_CONFIG, APP_CONFIG, logger
from src.tools.database_connector import DatabaseConnector
from src.tools.schema_discoverer import SchemaDiscoverer
from src.tools.data_profiler import DataProfiler
from src.agents.documentation_agent import DocumentationAgent
from src.tools.ui_generator import UIGenerator

class CatalogPipeline:
    """Orchestrates the complete data catalog generation pipeline"""
    
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.schema_discoverer = SchemaDiscoverer(self.db_connector)
        self.data_profiler = DataProfiler(self.db_connector)
        self.documentation_agent = DocumentationAgent()
    
    def run(self) -> str:
        """Execute the complete pipeline"""
        logger.info("Starting database catalog generation pipeline")
        
        try:
            # Step 1: Connect to database
            logger.info("Step 1: Connecting to database")
            if not self.db_connector.connect():
                raise Exception("Failed to connect to database")
            
            # Step 2: Discover schema
            logger.info("Step 2: Discovering database schema")
            tables_df = self.schema_discoverer.discover_tables()
            if tables_df.empty:
                raise Exception("No tables found in schema")
            
            table_names = tables_df['name'].tolist()
            logger.info(f"Found {len(table_names)} tables")
            
            # Step 3: Collect metadata
            logger.info("Step 3: Collecting column metadata")
            metadata_df = self.schema_discoverer.get_table_metadata(table_names)
            if metadata_df.empty:
                raise Exception("No column metadata found")
            
            # Step 4: Analyze column roles
            logger.info("Step 4: Analyzing column roles")
            analyzed_df = self.schema_discoverer.analyze_column_roles(metadata_df)
            
            # Step 5: Profile data
            logger.info("Step 5: Profiling data samples")
            profiled_df = self.data_profiler.profile_columns(analyzed_df)
            
            # Step 6: Generate documentation (AI agent)
            logger.info("Step 6: Generating AI documentation")
            documented_df = self.documentation_agent.generate_documentation(profiled_df)
            
            # Step 7: Save final dataset
            logger.info("Step 7: Saving final data dictionary")
            output_file = APP_CONFIG.get_output_path(
                APP_CONFIG.get_timestamped_filename('final_data_dictionary', 'csv')
            )
            documented_df.to_csv(output_file, index=False)
            logger.info(f"Final data dictionary saved to {output_file}")
            
            # Step 8: Create UI
            logger.info("Step 8: Creating web interface")
            ui_generator = UIGenerator(documented_df)
            url = ui_generator.create_interface()
            
            logger.info(f"Pipeline completed successfully! Access catalog at: {url}")
            return url
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.db_connector.close()
EOF

# Create main script
cat > main.py << 'EOF'
"""Main script to run the database catalog generator"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.core.config import DB_CONFIG, logger
from src.core.pipeline import CatalogPipeline

def main():
    """Run the database catalog generation pipeline"""
    print("Database Catalog Generator")
    print("=" * 50)
    
    try:
        # Validate configuration
        print(f"Target: {DB_CONFIG.database}.{DB_CONFIG.schema_name}")
        DB_CONFIG.get_connection_params()  # Will raise error if invalid
        
        # Run pipeline
        pipeline = CatalogPipeline()
        url = pipeline.run()
        
        print(f"\nSuccess! Your data catalog is available at: {url}")
        
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("Please check your .env file and ensure all required fields are set.")
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        logger.exception("Pipeline execution failed")

if __name__ == "__main__":
    main()
EOF

# Create test script
cat > test_components.py << 'EOF'
"""Test individual components"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.tools.database_connector import DatabaseConnector
from src.core.config import logger

def test_connection():
    """Test database connection"""
    print("Testing database connection...")
    
    connector = DatabaseConnector()
    if connector.connect():
        print("✅ Connection successful")
        
        # Test query
        result = connector.execute_query("SELECT CURRENT_VERSION()")
        print(f"Snowflake version: {result[0]['CURRENT_VERSION()']}")
        
        connector.close()
        return True
    else:
        print("❌ Connection failed")
        return False

if __name__ == "__main__":
    test_connection()
EOF

# Create README
cat > README.md << 'EOF'
# Database Catalog Generator

Simplified database catalog generation using AI for documentation.

## Architecture

- **Tools**: Simple utilities for data processing (connection, schema discovery, profiling)
- **Agent**: Single CrewAI agent for AI-powered documentation
- **Pipeline**: Orchestrator that ties everything together

## Setup

1. Copy environment configuration:
   ```bash
   cp .env.template .env
   # Edit .env with your Snowflake and AI credentials
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Test connection:
   ```bash
   python test_components.py
   ```

4. Run full pipeline:
   ```bash
   python main.py
   ```

## Output

- Interactive web interface at http://localhost:7860
- Final data dictionary CSV in outputs/
- Comprehensive logs

## Architecture Benefits

- **Simple**: Only uses CrewAI where it adds value (AI reasoning)
- **Fast**: Direct data processing without agent overhead
- **Maintainable**: Clear separation between tools and intelligent agents
- **Extensible**: Easy to add new profiling tools or documentation features
EOF

# Create gitignore
cat > .gitignore << 'EOF'
# Environment
.env
__pycache__/
*.pyc
*.pyo
*.pyd
.Python

# Output files
outputs/
logs/
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
EOF

# Make main script executable
chmod +x main.py
chmod +x test_components.py

echo "✅ Database Catalog project created successfully!"
echo ""
echo "📁 Project structure:"
echo "   $PROJECT_NAME/"
echo "   ├── src/"
echo "   │   ├── tools/          # Simple utilities"
echo "   │   ├── agents/         # Single AI agent"
echo "   │   └── core/           # Configuration & pipeline"
echo "   ├── outputs/            # Generated files"
echo "   ├── main.py            # Run the pipeline"
echo "   └── requirements.txt    # Dependencies"
echo ""
echo "🚀 Next steps:"
echo "   1. cd $PROJECT_NAME"
echo "   2. pip install -r requirements.txt"
echo "   3. cp .env.template .env"
echo "   4. Edit .env with your credentials"
echo "   5. python test_components.py  # Test connection"
echo "   6. python main.py             # Run full pipeline"
echo ""
echo "📊 Architecture: Tools + Single AI Agent + Pipeline"
echo "   - Much simpler than the original over-engineered version"
echo "   - Uses CrewAI only where it adds real value"
echo "   - Faster execution with same functionality"
EOF

chmod +x setup.sh

echo "Created setup.sh script. Run with:"
echo "./setup.sh"