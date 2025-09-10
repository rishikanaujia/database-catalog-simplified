"""Configuration management for Database Catalog"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, field_validator

load_dotenv()

class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema_name: str = ""
    role: Optional[str] = None
    region: Optional[str] = None
    authenticator: str = "snowflake"

    def __init__(self, **kwargs):
        super().__init__(
            account=kwargs.get('account', os.getenv("SNOWFLAKE_ACCOUNT", "")),
            user=kwargs.get('user', os.getenv("SNOWFLAKE_USER", "")),
            password=kwargs.get('password', os.getenv("SNOWFLAKE_PASSWORD", "")),
            warehouse=kwargs.get('warehouse', os.getenv("SNOWFLAKE_WAREHOUSE", "")),
            database=kwargs.get('database', os.getenv("SNOWFLAKE_DATABASE", "")),
            schema_name=kwargs.get('schema_name', os.getenv("SNOWFLAKE_SCHEMA", "")),
            role=kwargs.get('role', os.getenv("SNOWFLAKE_ROLE")),
            region=kwargs.get('region', os.getenv("SNOWFLAKE_REGION")),
            authenticator=kwargs.get('authenticator', os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake"))
        )

    @field_validator('account', 'user', 'password', 'warehouse', 'database', 'schema_name')
    @classmethod
    def validate_required_fields(cls, v: str, info):
        if not v:
            raise ValueError(f"{info.field_name} is required")
        return v

    def get_connection_params(self):
        """Return connection parameters for snowflake.connector"""
        params = {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema_name,
            'authenticator': self.authenticator
        }
        if self.role:
            params['role'] = self.role
        if self.region:
            params['region'] = self.region
        return params

class AppConfig(BaseModel):
    """Application configuration"""
    log_level: str = "INFO"
    sample_size: int = 1000
    batch_size: int = 10
    output_dir: str = "./outputs"
    anthropic_api_key: Optional[str] = None
    model_name: str = "claude-3-haiku-20240307"

    def __init__(self, **kwargs):
        super().__init__(
            log_level=kwargs.get('log_level', os.getenv("LOG_LEVEL", "INFO")),
            sample_size=kwargs.get('sample_size', int(os.getenv("SAMPLE_SIZE", "1000"))),
            batch_size=kwargs.get('batch_size', int(os.getenv("BATCH_SIZE", "10"))),
            output_dir=kwargs.get('output_dir', os.getenv("OUTPUT_DIR", "./outputs")),
            anthropic_api_key=kwargs.get('anthropic_api_key', os.getenv("ANTHROPIC_API_KEY")),
            model_name=kwargs.get('model_name', os.getenv("MODEL_NAME", "claude-3-haiku-20240307"))
        )
        
        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def get_timestamped_filename(self, base_name: str, extension: str = "csv") -> str:
        """Generate timestamped filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{timestamp}.{extension}"

    def get_output_path(self, filename: str) -> Path:
        """Return full path for an output file"""
        return Path(self.output_dir) / filename

# Global config instances
DB_CONFIG = DatabaseConfig()
APP_CONFIG = AppConfig()

# Setup logging
def setup_logging():
    logging.basicConfig(
        level=getattr(logging, APP_CONFIG.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(APP_CONFIG.get_output_path("catalog.log"))
        ]
    )
    return logging.getLogger("database_catalog")

logger = setup_logging()
