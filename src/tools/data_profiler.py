"""Data profiling tool for sampling and statistics - Updated with configuration"""

import pandas as pd
from typing import Dict, Any, List
from src.core.config import DB_CONFIG, APP_CONFIG, logger
from src.tools.database_connector import DatabaseConnector

# NEW: Import the data processing config
from src.core.data_processing_config import DATA_PROCESSING_CONFIG

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
        
        # Determine sampling strategy using config
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
            return DATA_PROCESSING_CONFIG.large_table_threshold  # Use config default assumption
    
    def _get_sample_clause(self, row_count: int) -> str:
        """Determine sampling strategy based on table size - UPDATED to use config"""
        return DATA_PROCESSING_CONFIG.get_sample_clause(row_count)
    
    def _profile_numeric_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile numeric column - UPDATED with configurable formatting"""
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
            row = result[0]
            # Format numeric values using config
            formatted_result = {}
            for key, value in row.items():
                if key.lower() in ['min_value', 'max_value', 'avg_value'] and value is not None:
                    formatted_result[key.lower()] = DATA_PROCESSING_CONFIG.format_numeric_value(float(value))
                else:
                    formatted_result[key.lower()] = value
            return formatted_result
        return {}
    
    def _profile_text_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile text column - UPDATED to use configurable limits"""
        # Use configurable max distinct values
        max_values = DATA_PROCESSING_CONFIG.max_distinct_values
        
        # First check how many distinct values exist
        count_query = f"""
        SELECT COUNT(DISTINCT {column_name}) as distinct_count
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        """
        
        count_result = self.db.execute_query(count_query)
        distinct_count = count_result[0].get('DISTINCT_COUNT', 0) if count_result else 0
        
        # Only get all values if reasonable number, otherwise limit using config
        limit_clause = "" if distinct_count <= max_values else f"LIMIT {max_values}"
        
        query = f"""
        SELECT DISTINCT {column_name}
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        ORDER BY {column_name}
        {limit_clause}
        """
        
        result = self.db.execute_query(query)
        if result:
            # Handle case where column name might be uppercase in result
            sample_values = []
            for row in result:
                # Try both cases
                value = row.get(column_name) or row.get(column_name.upper())
                if value:
                    # Truncate individual values using config
                    str_value = str(value)
                    if len(str_value) > DATA_PROCESSING_CONFIG.max_individual_value_length:
                        str_value = str_value[:DATA_PROCESSING_CONFIG.max_individual_value_length] + "..."
                    sample_values.append(str_value)
            
            # Join values and apply total length limit using config
            values_text = '; '.join(sample_values)
            if len(values_text) > DATA_PROCESSING_CONFIG.max_sample_text_length:
                values_text = values_text[:DATA_PROCESSING_CONFIG.max_sample_text_length] + "..."
            
            # Add indicator if truncated using config
            if distinct_count > max_values:
                truncation_msg = f"{DATA_PROCESSING_CONFIG.truncation_indicator}({distinct_count} total distinct values)"
                
                # Ensure we don't exceed length limit
                if len(values_text) + len(truncation_msg) > DATA_PROCESSING_CONFIG.max_sample_text_length:
                    values_text = values_text[:DATA_PROCESSING_CONFIG.max_sample_text_length - len(truncation_msg)]
                
                values_text += truncation_msg
                
            return {'sample_values': values_text, 'distinct_count': distinct_count}
        return {}
    
    def _profile_date_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile date column (unchanged but could add config for date formatting)"""
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
            row = result[0]
            return {
                'min_value': str(row.get('MIN_VALUE') or row.get('min_value')) if row.get('MIN_VALUE') or row.get('min_value') else None,
                'max_value': str(row.get('MAX_VALUE') or row.get('max_value')) if row.get('MAX_VALUE') or row.get('max_value') else None,
                'distinct_count': row.get('DISTINCT_COUNT') or row.get('distinct_count')
            }
        return {}