#!/usr/bin/env python3
"""
Standalone UI launcher for existing data dictionary CSV files
Usage: python view_catalog.py [csv_file_path]
"""

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.tools.ui_generator import UIGenerator
from src.core.config import logger

def main():
    """Launch UI with existing CSV data"""
    
    # Default to the most recent file if no argument provided
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = "outputs/final_data_dictionary_20250912_005835.csv"
    
    csv_path = Path(csv_file)
    
    # Check if file exists
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        print("\nAvailable files in outputs directory:")
        outputs_dir = Path("outputs")
        if outputs_dir.exists():
            csv_files = list(outputs_dir.glob("final_data_dictionary_*.csv"))
            if csv_files:
                for f in sorted(csv_files, reverse=True):  # Most recent first
                    print(f"  {f}")
                print(f"\nTo use a specific file: python {sys.argv[0]} <csv_file_path>")
            else:
                print("  No data dictionary CSV files found")
        return 1
    
    try:
        # Load the CSV data
        print(f"Loading data from: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Validate expected columns
        required_columns = ['table_name', 'column_name', 'business_data_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Error: CSV missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return 1
        
        print(f"Loaded {len(df)} columns from {df['table_name'].nunique()} tables")
        
        # Create and launch UI
        ui_generator = UIGenerator(df)
        
        # Fix the Gradio color issue by updating the UI config temporarily
        from src.core.ui_config import UI_CONFIG
        
        # Override the problematic color method
        def get_gradio_theme_config_fixed():
            return {
                'primary_hue': 'blue',
                'secondary_hue': 'yellow', 
                'neutral_hue': 'gray'
            }
        
        # Monkey patch the method
        UI_CONFIG.get_gradio_theme_config = get_gradio_theme_config_fixed
        
        url = ui_generator.create_interface()
        
        print(f"Data catalog UI launched successfully!")
        print(f"Access at: {url}")
        print(f"Press Ctrl+C to stop the server")
        
        # Keep the script running
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            return 0
            
    except Exception as e:
        print(f"Error loading or displaying data: {e}")
        logger.exception("Failed to launch UI")
        return 1

if __name__ == "__main__":
    sys.exit(main())