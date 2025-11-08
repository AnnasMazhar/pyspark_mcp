# PySpark Tools MCP Server

Convert SQL to optimized PySpark code with multi-dialect support, AWS Glue integration, and intelligent optimization recommendations.

## MCP Server Setup

### Quick Start (Recommended)

```bash
# Install and run
uvx pyspark-tools-mcp-server

# Or with uv
uv tool install pyspark-tools-mcp-server
pyspark-tools-mcp-server
```

### IDE Integration

Add to your MCP configuration (`.kiro/settings/mcp.json` or `~/.kiro/settings/mcp.json`):

```json
{
  "mcpServers": {
    "pyspark-tools": {
      "command": "uvx",
      "args": ["pyspark-tools-mcp-server"],
      "env": {
        "PYSPARK_TOOLS_DB_PATH": "~/.cache/mcp/pyspark_tools.sqlite"
      }
    }
  }
}
```

### Docker Setup

```bash
# Run server
docker run -p 8000:8000 pyspark-tools:latest

# Or with docker-compose
docker-compose up -d
```

## Core Features

- **Multi-Dialect SQL Conversion** - PostgreSQL, Oracle, Redshift, Spark SQL
- **AWS Glue Integration** - DynamicFrame support, job templates, Data Catalog
- **Intelligent Optimization** - Performance analysis, join strategies, partitioning
- **Batch Processing** - Concurrent file processing with progress tracking
- **Code Review** - Best practices analysis and security validation

## MCP Tools (24+ Available)

### SQL Conversion
- `convert_sql_to_pyspark` - Convert SQL with dialect detection
- `optimize_pyspark_code` - Performance optimization suggestions
- `review_pyspark_code` - Code quality analysis

### AWS Glue
- `generate_aws_glue_job_template` - Complete job templates
- `generate_data_catalog_table_definition` - Table definitions
- `convert_dataframe_to_dynamic_frame` - DynamicFrame conversion

### Batch Operations
- `batch_process_files` - Process multiple SQL files
- `batch_process_directory` - Process entire directories
- `extract_sql_from_pdf` - Extract SQL from PDFs

### Analysis & Optimization
- `analyze_data_flow` - Data flow analysis
- `recommend_join_strategy` - Join optimization
- `suggest_partitioning_strategy` - Partitioning recommendations

## Usage Examples

### Basic SQL Conversion
```python
# Via MCP tool
result = convert_sql_to_pyspark(
    sql_query="SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    dialect="postgres"
)
```

### AWS Glue Job Generation
```python
# Generate complete Glue job
job = generate_aws_glue_job_template(
    job_name="etl-orders",
    source_database="raw_data",
    source_table="orders",
    target_database="processed_data",
    target_table="customer_totals"
)
```

### Batch Processing
```python
# Process directory of SQL files
result = batch_process_directory(
    directory_path="/path/to/sql/files",
    output_dir="/path/to/output",
    recursive=True
)
```

## Local Development

```bash
# Clone and setup
git clone <repo-url>
cd pyspark_tools
pip install -r requirements.txt

# Run server
python run_server.py

# Run tests
make test-all

# Format code
make format
```

## Configuration

Environment variables:
- `PYSPARK_TOOLS_DB_PATH` - Database location (default: `~/.cache/mcp/memory.sqlite`)
- `PYSPARK_TOOLS_OUTPUT_DIR` - Output directory for batch processing
- `PYSPARK_TOOLS_CACHE_DIR` - Cache directory

## Performance

- **<2s** SQL conversion time
- **<5s** server startup
- **100+** concurrent file processing
- **>95%** conversion success rate

## Documentation

- [API Reference](docs/api.md) - Complete MCP tool documentation
- [Installation Guide](docs/installation.md) - Detailed setup instructions
- [Usage Examples](docs/usage.md) - Real-world examples
- [Security Report](SECURITY_AUDIT_REPORT.md) - Security analysis

## License

MIT License
