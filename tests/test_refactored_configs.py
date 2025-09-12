#!/usr/bin/env python3
"""
Test script for refactored configuration files
Run after migrating to BaseConfig to ensure everything works
"""

import sys
from pathlib import Path

# Add src to path (from tests directory)
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(src_path))

def test_refactored_configs():
    """Test all refactored config files"""
    print("🔧 Testing Refactored Configuration Files")
    print("=" * 50)
    
    success_count = 0
    total_tests = 0
    
    # Test 1: Import all configs
    print("\n📦 Test 1: Importing Refactored Configs")
    try:
        from src.core.base_config import BaseConfig
        from src.core.data_processing_config import DATA_PROCESSING_CONFIG
        from src.core.ui_config import UI_CONFIG
        from src.core.column_config import COLUMN_CONFIG
        
        print("✅ All refactored configs imported successfully")
        success_count += 1
    except Exception as e:
        print(f"❌ Import failed: {e}")
    total_tests += 1
    
    # Test 2: Test BaseConfig features
    print("\n🔧 Test 2: BaseConfig Features")
    try:
        # Test dot notation
        port = UI_CONFIG.get('server.port', 7860)
        sample_rows = DATA_PROCESSING_CONFIG.get('sampling.max_sample_rows', 5000)
        pk_suffixes = COLUMN_CONFIG.get('primary_keys.suffixes', [])
        
        print(f"✅ Dot notation works - Port: {port}, Sample rows: {sample_rows}")
        print(f"✅ PK suffixes: {pk_suffixes}")
        success_count += 1
    except Exception as e:
        print(f"❌ BaseConfig features failed: {e}")
    total_tests += 1
    
    # Test 3: Backwards compatibility 
    print("\n🔄 Test 3: Backwards Compatibility")
    try:
        # All old properties should still work
        assert DATA_PROCESSING_CONFIG.max_sample_rows > 0
        assert UI_CONFIG.port > 0
        assert len(COLUMN_CONFIG.primary_key_suffixes) > 0
        
        print("✅ All old properties still work")
        print(f"   - Max sample rows: {DATA_PROCESSING_CONFIG.max_sample_rows}")
        print(f"   - UI port: {UI_CONFIG.port}")
        print(f"   - Primary key suffixes: {COLUMN_CONFIG.primary_key_suffixes}")
        success_count += 1
    except Exception as e:
        print(f"❌ Backwards compatibility failed: {e}")
    total_tests += 1
    
    # Test 4: Configuration validation
    print("\n✅ Test 4: Configuration Validation")
    try:
        data_valid = DATA_PROCESSING_CONFIG.validate()
        ui_valid = UI_CONFIG.validate()
        column_valid = COLUMN_CONFIG.validate()
        
        print(f"✅ Data processing config valid: {data_valid}")
        print(f"✅ UI config valid: {ui_valid}")  
        print(f"✅ Column config valid: {column_valid}")
        
        if all([data_valid, ui_valid, column_valid]):
            success_count += 1
        else:
            print("❌ Some configs failed validation")
    except Exception as e:
        print(f"❌ Validation test failed: {e}")
    total_tests += 1
    
    # Test 5: New helper methods
    print("\n🛠️  Test 5: New Helper Methods") 
    try:
        # Test data processing helpers
        strategy = DATA_PROCESSING_CONFIG.get_sampling_strategy(1000)
        formatted_value = DATA_PROCESSING_CONFIG.format_numeric_value(123.456)
        
        # Test UI helpers
        table_style = UI_CONFIG.get_table_style()
        gradio_theme = UI_CONFIG.get_gradio_theme_config()
        
        # Test column classification helpers
        all_measures = COLUMN_CONFIG.get_all_measure_keywords()
        field_classification = COLUMN_CONFIG.classify_field_by_name('customer_id')
        
        print("✅ Data processing helpers work")
        print(f"   - Sampling strategy for 1000 rows: {strategy}")
        print(f"   - Formatted value: {formatted_value}")
        
        print("✅ UI helpers work")
        print(f"   - Gradio theme: {gradio_theme}")
        
        print("✅ Column classification helpers work")
        print(f"   - Measure keywords count: {len(all_measures)}")
        print(f"   - customer_id classification: {field_classification}")
        
        success_count += 1
    except Exception as e:
        print(f"❌ Helper methods test failed: {e}")
    total_tests += 1
    
    # Test 6: Config updating (non-destructive)
    print("\n🔄 Test 6: Runtime Configuration Updates")
    try:
        # Save original values
        original_port = UI_CONFIG.port
        original_batch_size = DATA_PROCESSING_CONFIG.batch_size
        
        # Test updating
        UI_CONFIG.update('server.port', 8080)  
        DATA_PROCESSING_CONFIG.update('profiling.batch_size', 15)
        
        # Verify updates
        assert UI_CONFIG.get('server.port') == 8080
        assert DATA_PROCESSING_CONFIG.get('profiling.batch_size') == 15
        
        # Restore original values
        UI_CONFIG.update('server.port', original_port)
        DATA_PROCESSING_CONFIG.update('profiling.batch_size', original_batch_size)
        
        print("✅ Runtime config updates work")
        print(f"   - Temporarily changed port to 8080, restored to {original_port}")
        print(f"   - Temporarily changed batch size to 15, restored to {original_batch_size}")
        
        success_count += 1
    except Exception as e:
        print(f"❌ Runtime updates test failed: {e}")
    total_tests += 1
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 REFACTOR TEST RESULTS")
    print(f"   ✅ Passed: {success_count}")
    print(f"   ❌ Failed: {total_tests - success_count}")
    print(f"   📊 Total: {total_tests}")
    
    if success_count == total_tests:
        print("\n🎉 All tests passed! Refactoring was successful.")
        print("   Your configs now use BaseConfig and have new features:")
        print("   • Better error handling")
        print("   • Dot notation access") 
        print("   • Configuration validation")
        print("   • Runtime config updates")
        print("   • All existing functionality preserved")
        return True
    else:
        print(f"\n⚠️  {total_tests - success_count} test(s) failed.")
        print("   Consider rolling back to backup files and trying migration again.")
        return False

def test_integration_with_existing_code():
    """Test that refactored configs work with existing pipeline code"""
    print("\n🔗 Integration Test: Pipeline Compatibility")
    
    try:
        # Test that the pipeline can still import and use configs
        from src.core.pipeline import CatalogPipeline
        from src.tools.schema_discoverer import SchemaDiscoverer
        from src.tools.data_profiler import DataProfiler
        from src.agents.documentation_agent import DocumentationAgent
        from src.tools.ui_generator import UIGenerator
        
        print("✅ All pipeline components can import configs")
        
        # Test that config values are accessible
        from src.core.data_processing_config import DATA_PROCESSING_CONFIG
        
        # These are used by the schema discoverer
        include_tables = DATA_PROCESSING_CONFIG.include_tables
        batch_size = DATA_PROCESSING_CONFIG.batch_size
        
        print(f"✅ Pipeline can access config values:")
        print(f"   - Include tables: {include_tables}")
        print(f"   - Batch size: {batch_size}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

def main():
    """Run all refactor tests"""
    config_test_passed = test_refactored_configs()
    integration_test_passed = test_integration_with_existing_code()
    
    if config_test_passed and integration_test_passed:
        print("\n🚀 MIGRATION SUCCESSFUL!")
        print("   You can now use your refactored configuration system.")
        print("   Run 'python main.py' to test the full pipeline.")
        return 0
    else:
        print("\n🔄 MIGRATION ISSUES DETECTED")
        print("   Consider rolling back to backup files:")
        print("   cp src/core/*.backup src/core/")
        return 1

if __name__ == "__main__":
    sys.exit(main())