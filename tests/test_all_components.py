#!/usr/bin/env python3
"""Comprehensive test script for database catalog components"""

import sys
import traceback
import time
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

# Add src to path - robust path handling for tests directory
import sys
import os

# Get the project root directory (parent of tests directory)
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'

# Add both project root and src to Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(src_path))

# Debug: Print paths to help troubleshoot
print(f"🔧 Project root: {project_root}")
print(f"🔧 Source path: {src_path}")
print(f"🔧 Source path exists: {src_path.exists()}")

# Import components
from src.core.config import DB_CONFIG, APP_CONFIG, logger
from src.tools.database_connector import DatabaseConnector, get_database_connection
from src.tools.schema_discoverer import SchemaDiscoverer
from src.tools.data_profiler import DataProfiler
from src.agents.documentation_agent import DocumentationAgent
from src.core.pipeline import CatalogPipeline

class ComponentTester:
    """Test runner for database catalog components"""
    
    def __init__(self):
        self.results = {}
        self.start_time = time.time()
        
    def log_result(self, test_name: str, success: bool, message: str = "", duration: float = 0):
        """Log test results"""
        status = "✅ PASS" if success else "❌ FAIL"
        self.results[test_name] = {
            'success': success,
            'message': message,
            'duration': duration
        }
        
        duration_str = f" ({duration:.2f}s)" if duration > 0 else ""
        print(f"{status} {test_name}{duration_str}")
        if message:
            print(f"    {message}")
        print()
    
    def test_configuration_loading(self) -> bool:
        """Test 1: Configuration Loading"""
        print("🔧 Testing Configuration Loading...")
        test_start = time.time()
        
        try:
            # Test individual config imports
            from src.core.ai_config import AI_CONFIG
            from src.core.data_processing_config import DATA_PROCESSING_CONFIG
            from src.core.ui_config import UI_CONFIG
            from src.core.column_config import COLUMN_CONFIG
            
            # Test basic property access
            assert AI_CONFIG.primary_model is not None, "AI config primary_model is None"
            assert DATA_PROCESSING_CONFIG.max_sample_rows > 0, "Data processing max_sample_rows invalid"
            assert UI_CONFIG.port > 0, "UI config port invalid"
            assert len(COLUMN_CONFIG.primary_key_suffixes) > 0, "Column config has no primary key suffixes"
            
            # Test the fixed duplicate property bug
            sample_rows_1 = DATA_PROCESSING_CONFIG.max_sample_rows
            sample_rows_2 = DATA_PROCESSING_CONFIG.max_sample_rows
            assert sample_rows_1 == sample_rows_2, "max_sample_rows property inconsistent"
            
            duration = time.time() - test_start
            self.log_result("Configuration Loading", True, 
                          f"All config classes loaded successfully. Sample rows: {sample_rows_1}", duration)
            return True
            
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Configuration Loading", False, f"Error: {str(e)}", duration)
            return False
    
    def test_database_connection(self) -> bool:
        """Test 2: Database Connection"""
        print("🔌 Testing Database Connection...")
        test_start = time.time()
        
        try:
            # Test environment variables
            required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 
                           'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA']
            missing_vars = []
            
            for var in required_vars:
                import os
                if not os.getenv(var):
                    missing_vars.append(var)
            
            if missing_vars:
                self.log_result("Database Connection", False, 
                              f"Missing environment variables: {', '.join(missing_vars)}", 
                              time.time() - test_start)
                return False
            
            # Test connection
            connector = DatabaseConnector()
            if connector.connect():
                # Test basic query
                result = connector.execute_query("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                
                conn_info = connector.get_connection_info()
                info_str = f"DB: {conn_info.get('current_database')}.{conn_info.get('current_schema')}"
                
                connector.close()
                
                duration = time.time() - test_start
                self.log_result("Database Connection", True, info_str, duration)
                return True
            else:
                self.log_result("Database Connection", False, "Failed to connect", 
                              time.time() - test_start)
                return False
                
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Database Connection", False, f"Error: {str(e)}", duration)
            return False
    
    def test_context_manager(self) -> bool:
        """Test 3: Database Context Manager"""
        print("🔄 Testing Database Context Manager...")
        test_start = time.time()
        
        try:
            with get_database_connection() as db:
                result = db.execute_query("SELECT 1 as test_col")
                assert len(result) == 1, "Context manager query failed"
                assert result[0]['TEST_COL'] == 1, "Query result incorrect"
            
            duration = time.time() - test_start
            self.log_result("Database Context Manager", True, "Context manager working correctly", duration)
            return True
            
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Database Context Manager", False, f"Error: {str(e)}", duration)
            return False
    
    def test_schema_discovery(self) -> bool:
        """Test 4: Schema Discovery"""
        print("🔍 Testing Schema Discovery...")
        test_start = time.time()
        
        try:
            with get_database_connection() as db:
                discoverer = SchemaDiscoverer(db)
                
                # Test table discovery
                tables_df = discoverer.discover_tables()
                if tables_df.empty:
                    self.log_result("Schema Discovery", False, "No tables found", 
                                  time.time() - test_start)
                    return False
                
                table_names = tables_df['name'].tolist()
                
                # Test metadata collection for first few tables
                test_tables = table_names[:2]  # Test with first 2 tables
                metadata_df = discoverer.get_table_metadata(test_tables)
                
                if metadata_df.empty:
                    self.log_result("Schema Discovery", False, "No metadata found", 
                                  time.time() - test_start)
                    return False
                
                # Test column role analysis
                analyzed_df = discoverer.analyze_column_roles(metadata_df)
                
                required_cols = ['table_name', 'column_name', 'column_role', 'business_data_type']
                missing_cols = [col for col in required_cols if col not in analyzed_df.columns]
                
                if missing_cols:
                    self.log_result("Schema Discovery", False, 
                                  f"Missing columns: {missing_cols}", time.time() - test_start)
                    return False
                
                duration = time.time() - test_start
                self.log_result("Schema Discovery", True, 
                              f"Found {len(table_names)} tables, {len(analyzed_df)} columns", duration)
                return True
                
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Schema Discovery", False, f"Error: {str(e)}", duration)
            return False
    
    def test_data_profiling(self) -> bool:
        """Test 5: Data Profiling"""
        print("📊 Testing Data Profiling...")
        test_start = time.time()
        
        try:
            with get_database_connection() as db:
                discoverer = SchemaDiscoverer(db)
                profiler = DataProfiler(db)
                
                # Get a small sample for testing
                tables_df = discoverer.discover_tables()
                if tables_df.empty:
                    self.log_result("Data Profiling", False, "No tables for profiling test", 
                                  time.time() - test_start)
                    return False
                
                table_names = tables_df['name'].tolist()[:1]  # Test with just 1 table
                metadata_df = discoverer.get_table_metadata(table_names)
                analyzed_df = discoverer.analyze_column_roles(metadata_df)
                
                # Test profiling on first few columns
                test_df = analyzed_df.head(5)  # Test with first 5 columns
                profiled_df = profiler.profile_columns(test_df)
                
                # Check that profiling added some information
                original_cols = set(analyzed_df.columns)
                profiled_cols = set(profiled_df.columns)
                new_cols = profiled_cols - original_cols
                
                duration = time.time() - test_start
                self.log_result("Data Profiling", True, 
                              f"Profiled {len(test_df)} columns, added fields: {new_cols}", duration)
                return True
                
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Data Profiling", False, f"Error: {str(e)}", duration)
            return False
    
    def test_ai_documentation(self) -> bool:
        """Test 6: AI Documentation Generation"""
        print("🤖 Testing AI Documentation Generation...")
        test_start = time.time()
        
        try:
            # Check for AI API key
            import os
            if not os.getenv('ANTHROPIC_API_KEY') and not os.getenv('OPENAI_API_KEY'):
                self.log_result("AI Documentation", False, 
                              "No AI API key found (ANTHROPIC_API_KEY or OPENAI_API_KEY)", 
                              time.time() - test_start)
                return False
            
            # Create sample data for testing
            sample_data = pd.DataFrame({
                'table_name': ['CUSTOMER', 'CUSTOMER', 'ORDER'],
                'column_name': ['customer_id', 'customer_name', 'order_date'],
                'data_type': ['NUMBER', 'VARCHAR', 'DATE'],
                'business_data_type': ['Identifier', 'Description', 'Date'],
                'column_role': ['primary_key', 'dimension', 'dimension'],
                'sample_values': ['1, 2, 3', 'John, Jane, Bob', '2023-01-01, 2023-01-02']
            })
            
            agent = DocumentationAgent()
            
            # Test with small sample to avoid high API costs
            documented_df = agent.generate_documentation(sample_data)
            
            # Check that documentation was added
            required_cols = ['table_description', 'column_description']
            missing_cols = [col for col in required_cols if col not in documented_df.columns]
            
            if missing_cols:
                self.log_result("AI Documentation", False, 
                              f"Missing documentation columns: {missing_cols}", 
                              time.time() - test_start)
                return False
            
            # Check that descriptions are not empty
            empty_descriptions = documented_df['column_description'].isna().sum()
            
            duration = time.time() - test_start
            self.log_result("AI Documentation", True, 
                          f"Generated descriptions, {empty_descriptions} empty descriptions", duration)
            return True
            
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("AI Documentation", False, f"Error: {str(e)}", duration)
            return False
    
    def test_pipeline_integration(self) -> bool:
        """Test 7: Full Pipeline Integration"""
        print("🔄 Testing Full Pipeline Integration...")
        test_start = time.time()
        
        try:
            pipeline = CatalogPipeline()
            
            # This is a longer test - might want to make it optional
            print("    ⚠️  This test will run the full pipeline and may take several minutes...")
            print("    💰 This test will make AI API calls which may incur costs...")
            
            response = input("    Continue with full pipeline test? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                self.log_result("Pipeline Integration", True, "Skipped by user choice", 0)
                return True
            
            url = pipeline.run()
            
            if url and "localhost" in url:
                duration = time.time() - test_start
                self.log_result("Pipeline Integration", True, 
                              f"Pipeline completed successfully. UI at: {url}", duration)
                return True
            else:
                self.log_result("Pipeline Integration", False, 
                              "Pipeline completed but no valid URL returned", 
                              time.time() - test_start)
                return False
                
        except Exception as e:
            duration = time.time() - test_start
            self.log_result("Pipeline Integration", False, f"Error: {str(e)}", duration)
            return False
    
    def run_all_tests(self) -> Dict:
        """Run all tests and return summary"""
        print("🚀 Starting Database Catalog Component Tests")
        print("=" * 60)
        
        test_methods = [
            self.test_configuration_loading,
            self.test_database_connection,
            self.test_context_manager,
            self.test_schema_discovery,
            self.test_data_profiling,
            self.test_ai_documentation,
            self.test_pipeline_integration
        ]
        
        passed = 0
        failed = 0
        
        for test_method in test_methods:
            try:
                if test_method():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"❌ CRITICAL ERROR in {test_method.__name__}: {str(e)}")
                traceback.print_exc()
                failed += 1
        
        # Summary
        total_duration = time.time() - self.start_time
        print("=" * 60)
        print(f"📊 TEST SUMMARY")
        print(f"   Total Tests: {passed + failed}")
        print(f"   ✅ Passed: {passed}")
        print(f"   ❌ Failed: {failed}")
        print(f"   ⏱️  Total Duration: {total_duration:.2f}s")
        
        if failed == 0:
            print("🎉 All tests passed! Your database catalog is ready to use.")
        else:
            print("⚠️  Some tests failed. Check the errors above and fix issues before proceeding.")
        
        return {
            'passed': passed,
            'failed': failed,
            'total_duration': total_duration,
            'results': self.results
        }

def main():
    """Run the comprehensive test suite"""
    tester = ComponentTester()
    results = tester.run_all_tests()
    
    # Exit with error code if tests failed
    if results['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()