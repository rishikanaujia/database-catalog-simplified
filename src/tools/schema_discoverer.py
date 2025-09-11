"""Schema discovery and metadata collection tool - Updated to use configuration"""

import pandas as pd
from typing import List, Dict, Any
from src.core.config import DB_CONFIG, logger
from src.tools.database_connector import DatabaseConnector

# NEW: Import the column classification config
from src.core.column_config import COLUMN_CONFIG

class SchemaDiscoverer:
    """Discovers database schema and collects metadata"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    def discover_tables(self) -> pd.DataFrame:
        """Discover all tables in the schema (unchanged)"""
        logger.info(f"Discovering tables in {DB_CONFIG.database}.{DB_CONFIG.schema_name}")
        
        query = f"""
        SHOW TABLES IN SCHEMA {DB_CONFIG.database}.{DB_CONFIG.schema_name}
        """
        
        tables_data = self.db.execute_query(query)
        
        # Convert to DataFrame
        if tables_data:
            df = pd.DataFrame(tables_data)
            # Convert column names to lowercase for consistency
            df.columns = df.columns.str.lower()
            logger.info(f"Found {len(df)} tables")
            return df
        else:
            return pd.DataFrame()
    
    def get_table_metadata(self, table_names: List[str]) -> pd.DataFrame:
        """Get detailed metadata for tables (unchanged)"""
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
        # Convert column names to lowercase for consistency
        df.columns = df.columns.str.lower()
        logger.info(f"Collected metadata for {len(df)} columns")
        return df
    
    def analyze_column_roles(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze and classify column roles - UPDATED to use configuration"""
        logger.info("Analyzing column roles and business types")
        
        def classify_column(row):
            """Column classification using configurable rules"""
            col_name = row['column_name'].lower()
            data_type = row['data_type'].upper()
            is_nullable = row['is_nullable']
            
            # PRIMARY KEY DETECTION (using config)
            if any(col_name.endswith(suffix) for suffix in COLUMN_CONFIG.primary_key_suffixes):
                if COLUMN_CONFIG.primary_key_requires_not_null and is_nullable == 'NO':
                    return 'primary_key', 'Identifier'
                elif not COLUMN_CONFIG.primary_key_requires_not_null:
                    return 'primary_key', 'Identifier'
            
            # FOREIGN KEY DETECTION (using config)
            if any(col_name.endswith(suffix) for suffix in COLUMN_CONFIG.foreign_key_suffixes):
                return 'foreign_key', 'Identifier'
            
            # MEASURE DETECTION (using config)
            # Amount/Currency fields
            if any(keyword in col_name for keyword in COLUMN_CONFIG.amount_keywords):
                return 'measure', COLUMN_CONFIG.get_amount_business_type()
            
            # Quantity fields
            if any(keyword in col_name for keyword in COLUMN_CONFIG.quantity_keywords):
                return 'measure', COLUMN_CONFIG.get_quantity_business_type()
            
            # DIMENSION DETECTION (using config)
            # Description fields
            if any(keyword in col_name for keyword in COLUMN_CONFIG.description_keywords):
                return 'dimension', COLUMN_CONFIG.get_description_business_type()
            
            # Status fields
            if any(keyword in col_name for keyword in COLUMN_CONFIG.status_keywords):
                return 'dimension', COLUMN_CONFIG.get_status_business_type()
            
            # Location fields (new!)
            if any(keyword in col_name for keyword in COLUMN_CONFIG.location_keywords):
                return 'dimension', COLUMN_CONFIG.get_location_business_type()
            
            # DEFAULT CLASSIFICATION (using config)
            # Use data type to determine business type
            if data_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                return 'dimension', COLUMN_CONFIG.get_business_type_for_sql_type(data_type)
            else:
                business_type = COLUMN_CONFIG.get_business_type_for_sql_type(data_type)
                role = 'dimension' if data_type in ['TEXT', 'VARCHAR'] else 'measure'
                return role, business_type
        
        # Apply classification
        roles_and_types = metadata_df.apply(classify_column, axis=1, result_type='expand')
        metadata_df['column_role'] = roles_and_types[0]
        metadata_df['business_data_type'] = roles_and_types[1]
        
        # Log classification summary
        role_summary = metadata_df['column_role'].value_counts()
        type_summary = metadata_df['business_data_type'].value_counts()
        logger.info(f"Classification summary - Roles: {dict(role_summary)}")
        logger.info(f"Classification summary - Types: {dict(type_summary)}")

        return metadata_df