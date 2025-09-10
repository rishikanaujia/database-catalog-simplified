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
