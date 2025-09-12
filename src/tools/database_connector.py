"""Enhanced database connection utility with comprehensive error handling"""

import time
import snowflake.connector
from snowflake.connector import DictCursor, DatabaseError, ProgrammingError, InterfaceError
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from src.core.config import DB_CONFIG, logger

class DatabaseConnectionError(Exception):
    """Custom exception for database connection issues"""
    pass

class DatabaseQueryError(Exception):
    """Custom exception for database query issues"""
    pass

class DatabaseConnector:
    """Enhanced database connector with comprehensive error handling"""
    
    def __init__(self):
        self.connection = None
        self.max_retries = 3
        self.connection_timeout = 30
        self.query_timeout = 60
        self._connection_attempts = 0
        self._is_connected = False
    
    def connect(self) -> bool:
        """Establish database connection with enhanced error handling and retry logic"""
        self._connection_attempts = 0
        
        for attempt in range(self.max_retries):
            self._connection_attempts += 1
            
            try:
                logger.info(f"Database connection attempt {attempt + 1}/{self.max_retries}")
                
                conn_params = DB_CONFIG.get_connection_params()
                conn_params.update({
                    'network_timeout': self.connection_timeout,
                    'client_session_keep_alive': True,
                    'autocommit': True,
                    'login_timeout': 30,
                    'ocsp_response_cache_filename': None  # Disable OCSP caching issues
                })
                
                self.connection = snowflake.connector.connect(**conn_params)
                
                # Test connection with a simple query
                self._test_connection()
                
                self._is_connected = True
                logger.info(f"Database connection established successfully on attempt {attempt + 1}")
                return True
                
            except snowflake.connector.errors.DatabaseError as e:
                error_msg = f"Database error on attempt {attempt + 1}: {str(e)}"
                logger.error(error_msg)
                
                # Check for specific database errors
                if "authentication" in str(e).lower():
                    logger.error("Authentication failed - check credentials")
                    break  # Don't retry auth errors
                elif "account" in str(e).lower():
                    logger.error("Account identifier issue - check SNOWFLAKE_ACCOUNT")
                    break  # Don't retry account errors
                    
            except snowflake.connector.errors.InterfaceError as e:
                error_msg = f"Interface error on attempt {attempt + 1}: {str(e)}"
                logger.error(error_msg)
                
                if "network" in str(e).lower() or "timeout" in str(e).lower():
                    logger.warning("Network/timeout issue - will retry")
                else:
                    logger.error("Interface configuration error - check connection parameters")
                    break
                    
            except Exception as e:
                error_msg = f"Unexpected error on attempt {attempt + 1}: {str(e)}"
                logger.error(error_msg)
                
            # Wait before retry (exponential backoff)
            if attempt < self.max_retries - 1:
                wait_time = min(2 ** attempt, 30)  # Cap at 30 seconds
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        self._is_connected = False
        error_msg = f"Failed to establish database connection after {self.max_retries} attempts"
        logger.error(error_msg)
        raise DatabaseConnectionError(error_msg)
    
    def _test_connection(self) -> None:
        """Test the database connection with a simple query"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.debug(f"Connected to Snowflake version: {result[0]}")
            else:
                raise DatabaseConnectionError("Connection test query returned no results")
                
        except Exception as e:
            raise DatabaseConnectionError(f"Connection test failed: {str(e)}")
    
    @property
    def is_connected(self) -> bool:
        """Check if database is connected and responsive"""
        if not self._is_connected or not self.connection:
            return False
        
        try:
            # Quick connection test
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            self._is_connected = False
            return False
    
    def execute_query(self, query: str, params: Optional[List] = None, timeout: Optional[int] = None) -> List[Dict]:
        """Execute query with enhanced error handling and timeout control"""
        if not self.is_connected:
            raise DatabaseConnectionError("Database not connected. Call connect() first.")
        
        query_timeout = timeout or self.query_timeout
        cursor = None
        
        try:
            cursor = self.connection.cursor(DictCursor)
            
            # Set query timeout if supported
            try:
                cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {query_timeout}")
            except Exception as e:
                logger.debug(f"Could not set query timeout: {e}")
            
            logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            logger.debug(f"Query returned {len(results)} rows")
            return results
            
        except snowflake.connector.errors.ProgrammingError as e:
            error_code = getattr(e, 'errno', None)
            error_msg = str(e)
            
            if error_code == 2003:  # Compilation error
                raise DatabaseQueryError(f"SQL compilation error: {error_msg}")
            elif "does not exist" in error_msg.lower():
                raise DatabaseQueryError(f"Object not found: {error_msg}")
            elif "timeout" in error_msg.lower():
                raise DatabaseQueryError(f"Query timeout after {query_timeout}s: {error_msg}")
            else:
                raise DatabaseQueryError(f"SQL programming error: {error_msg}")
                
        except snowflake.connector.errors.DatabaseError as e:
            raise DatabaseQueryError(f"Database error during query execution: {str(e)}")
            
        except snowflake.connector.errors.InterfaceError as e:
            # Connection might be lost
            self._is_connected = False
            raise DatabaseConnectionError(f"Connection lost during query: {str(e)}")
            
        except Exception as e:
            raise DatabaseQueryError(f"Unexpected error during query execution: {str(e)}")
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass  # Ignore cursor close errors
    
    def execute_query_with_retry(self, query: str, params: Optional[List] = None, 
                                max_retries: int = 2) -> List[Dict]:
        """Execute query with automatic retry on recoverable errors"""
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                return self.execute_query(query, params)
                
            except DatabaseConnectionError as e:
                last_exception = e
                if attempt < max_retries:
                    logger.warning(f"Connection lost, attempting to reconnect (attempt {attempt + 1})")
                    try:
                        self.connect()
                    except DatabaseConnectionError:
                        continue  # Try again on next iteration
                        
            except DatabaseQueryError as e:
                # Don't retry programming errors or timeouts
                if "compilation error" in str(e).lower() or "timeout" in str(e).lower():
                    raise e
                    
                last_exception = e
                if attempt < max_retries:
                    logger.warning(f"Query error, retrying (attempt {attempt + 1}): {str(e)}")
                    time.sleep(1)  # Brief pause before retry
        
        # If we get here, all retries failed
        raise last_exception or DatabaseQueryError("Query failed after retries")
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection"""
        if not self.is_connected:
            return {"status": "disconnected"}
        
        try:
            info = {
                "status": "connected",
                "attempts": self._connection_attempts,
                "account": DB_CONFIG.account,
                "user": DB_CONFIG.user,
                "database": DB_CONFIG.database,
                "schema": DB_CONFIG.schema_name,
                "warehouse": DB_CONFIG.warehouse
            }
            
            # Get session info
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                info.update({
                    "version": result[0],
                    "current_user": result[1],
                    "current_database": result[2],
                    "current_schema": result[3],
                    "current_warehouse": result[4]
                })
            
            return info
            
        except Exception as e:
            logger.warning(f"Could not get connection info: {e}")
            return {"status": "connected", "error": str(e)}
    
    def close(self) -> None:
        """Close database connection with proper cleanup"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.warning(f"Error while closing database connection: {e}")
            finally:
                self.connection = None
                self._is_connected = False

# Context manager for automatic connection management
@contextmanager
def get_database_connection():
    """Context manager for database connections with automatic cleanup"""
    connector = DatabaseConnector()
    try:
        if not connector.connect():
            raise DatabaseConnectionError("Failed to establish database connection")
        yield connector
    finally:
        connector.close()