"""Main pipeline orchestrator"""

import pandas as pd
from src.core.config import DB_CONFIG, APP_CONFIG, logger
from src.tools.database_connector import DatabaseConnector
from src.tools.schema_discoverer import SchemaDiscoverer
from src.tools.data_profiler import DataProfiler
from src.agents.documentation_agent import DocumentationAgent
from src.tools.ui_generator import UIGenerator

class CatalogPipeline:
    """Orchestrates the complete data catalog generation pipeline"""
    
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.schema_discoverer = SchemaDiscoverer(self.db_connector)
        self.data_profiler = DataProfiler(self.db_connector)
        self.documentation_agent = DocumentationAgent()
    
    def run(self) -> str:
        """Execute the complete pipeline"""
        logger.info("Starting database catalog generation pipeline")
        
        try:
            # Step 1: Connect to database
            logger.info("Step 1: Connecting to database")
            if not self.db_connector.connect():
                raise Exception("Failed to connect to database")
            
            # Step 2: Discover schema
            logger.info("Step 2: Discovering database schema")
            tables_df = self.schema_discoverer.discover_tables()
            if tables_df.empty:
                raise Exception("No tables found in schema")
            
            table_names = tables_df['name'].tolist()
            logger.info(f"Found {len(table_names)} tables")
            
            # Step 3: Collect metadata
            logger.info("Step 3: Collecting column metadata")
            metadata_df = self.schema_discoverer.get_table_metadata(table_names)
            if metadata_df.empty:
                raise Exception("No column metadata found")
            
            # Step 4: Analyze column roles
            logger.info("Step 4: Analyzing column roles")
            analyzed_df = self.schema_discoverer.analyze_column_roles(metadata_df)
            
            # Step 5: Profile data
            logger.info("Step 5: Profiling data samples")
            profiled_df = self.data_profiler.profile_columns(analyzed_df)
            
            # Step 6: Generate documentation (AI agent)
            logger.info("Step 6: Generating AI documentation")
            documented_df = self.documentation_agent.generate_documentation(profiled_df)
            
            # Step 7: Save final dataset
            logger.info("Step 7: Saving final data dictionary")
            output_file = APP_CONFIG.get_output_path(
                APP_CONFIG.get_timestamped_filename('final_data_dictionary', 'csv')
            )
            documented_df.to_csv(output_file, index=False)
            logger.info(f"Final data dictionary saved to {output_file}")
            
            # Step 8: Create UI
            logger.info("Step 8: Creating web interface")
            ui_generator = UIGenerator(documented_df)
            url = ui_generator.create_interface()
            
            logger.info(f"Pipeline completed successfully! Access catalog at: {url}")
            return url
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.db_connector.close()
