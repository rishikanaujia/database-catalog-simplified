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
