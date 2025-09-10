# Database Catalog Generator

Simplified database catalog generation using AI for documentation.

## Architecture

- **Tools**: Simple utilities for data processing (connection, schema discovery, profiling)
- **Agent**: Single CrewAI agent for AI-powered documentation
- **Pipeline**: Orchestrator that ties everything together

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

## Output

- Interactive web interface at http://localhost:7860
- Final data dictionary CSV in outputs/
- Comprehensive logs

## Architecture Benefits

- **Simple**: Only uses CrewAI where it adds value (AI reasoning)
- **Fast**: Direct data processing without agent overhead
- **Maintainable**: Clear separation between tools and intelligent agents
- **Extensible**: Easy to add new profiling tools or documentation features
