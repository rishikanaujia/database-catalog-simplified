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
            # Convert to lowercase column names for consistency
            return {k.lower(): v for k, v in result[0].items()}
        return {}
    
    def _profile_text_column(self, table_name: str, column_name: str, sample_clause: str) -> Dict:
        """Profile text column"""
        # First check how many distinct values exist
        count_query = f"""
        SELECT COUNT(DISTINCT {column_name}) as distinct_count
        FROM {DB_CONFIG.database}.{DB_CONFIG.schema_name}.{table_name}
        WHERE {column_name} IS NOT NULL
        {sample_clause}
        """
        
        count_result = self.db.execute_query(count_query)
        distinct_count = count_result[0].get('DISTINCT_COUNT', 0) if count_result else 0
        
        # Only get all values if reasonable number, otherwise limit
        limit_clause = "" if distinct_count <= 50 else "LIMIT 50"
        
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
                    sample_values.append(value)
            
            # Add indicator if truncated
            values_text = '; '.join(map(str, sample_values))
            if distinct_count > 50:
                values_text += f" ... ({distinct_count} total distinct values)"
                
            return {'sample_values': values_text}
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
            row = result[0]
            return {
                'min_value': str(row.get('MIN_VALUE') or row.get('min_value')) if row.get('MIN_VALUE') or row.get('min_value') else None,
                'max_value': str(row.get('MAX_VALUE') or row.get('max_value')) if row.get('MAX_VALUE') or row.get('max_value') else None,
                'distinct_count': row.get('DISTINCT_COUNT') or row.get('distinct_count')
            }
        return {}
