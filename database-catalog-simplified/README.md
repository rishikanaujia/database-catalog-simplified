# Database Catalog Generator - Simplified Architecture

Automated database catalog generation using AI for documentation with improved sample value collection.

## Architecture

- **Tools**: Simple utilities for data processing (connection, schema discovery, profiling)
- **Agent**: Single CrewAI agent for AI-powered documentation
- **Pipeline**: Orchestrator that ties everything together

## Key Features

- **Smart Sample Collection**: Automatically collects up to 50 unique values per text column
- **Performance Optimized**: Uses intelligent sampling for large tables
- **Column Name Compatibility**: Handles Snowflake's uppercase column names
- **Comprehensive Profiling**: Statistics for numeric, text, and date columns

## Setup

1. Copy environment configuration:
   ```bash
   cp .env.template .env
   # Edit .env with your Snowflake and AI credentials
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Test connection:
   ```bash
   python test_components.py
   ```

4. Run full pipeline:
   ```bash
   python main.py
   ```

## Sample Value Collection

The data profiler now:
- Collects up to 50 unique values per text column
- Shows total count if more values exist (e.g., "... (127 total distinct values)")
- Uses smart sampling for large tables
- Handles both small lookup tables and large fact tables efficiently

## Output

- Interactive web interface at http://localhost:7860
- Final data dictionary CSV with comprehensive sample values
- Detailed logs and AI-generated documentation

## Architecture Benefits

- **Simple**: Only uses CrewAI where it adds value (AI reasoning)
- **Fast**: Direct data processing without agent overhead
- **Maintainable**: Clear separation between tools and intelligent agents
- **Extensible**: Easy to add new profiling features or modify sample collection
- **Production Ready**: Handles enterprise-scale datasets efficiently
