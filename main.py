"""Main script to run the database catalog generator"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.core.config import DB_CONFIG, logger
from src.core.pipeline import CatalogPipeline

def main():
    """Run the database catalog generation pipeline"""
    print("Database Catalog Generator")
    print("=" * 50)
    
    try:
        # Validate configuration
        print(f"Target: {DB_CONFIG.database}.{DB_CONFIG.schema_name}")
        DB_CONFIG.get_connection_params()  # Will raise error if invalid
        
        # Run pipeline
        pipeline = CatalogPipeline()
        url = pipeline.run()
        
        print(f"\nSuccess! Your data catalog is available at: {url}")
        
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("Please check your .env file and ensure all required fields are set.")
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        logger.exception("Pipeline execution failed")

if __name__ == "__main__":
    main()
