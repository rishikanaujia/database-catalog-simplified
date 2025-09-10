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
