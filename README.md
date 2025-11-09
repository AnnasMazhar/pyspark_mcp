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

## MCP Tools (49 Available)

### Flagship Workflow Tool
- `complete_sql_conversion` - **One-call complete SQL to PySpark conversion with optimization**

### Core Conversion Tools (4 Tools)
- `convert_sql_to_pyspark` - Convert SQL with dialect detection
- `optimize_pyspark_code` - Performance optimization suggestions  
- `review_pyspark_code` - Code quality analysis
- `analyze_sql_context` - Auto-detect SQL patterns and complexity

### Workflow Integration Tools (6 Tools)
- `process_editor_selection` - IDE integration for real-time conversion
- `realtime_sql_assistance` - Live coding assistance as you type
- `workspace_analysis` - Workspace-wide optimization analysis
- `generate_project_structure` - Complete production project generation

### AWS Glue Integration Tools (11 Tools)
- `generate_aws_glue_job_template` - Complete job templates
- `generate_data_catalog_table_definition` - Table definitions
- `convert_dataframe_to_dynamic_frame` - DynamicFrame conversion
- `generate_glue_job_with_sql_conversion` - End-to-end Glue job creation
- `detect_schema_from_sample_data` - Automatic schema detection
- `generate_schema_evolution_strategy` - Schema change management
- `analyze_s3_optimization_opportunities` - S3 optimization analysis
- `generate_s3_optimization_strategy` - S3 optimization implementation
- `generate_small_files_consolidation_job` - Small files consolidation
- `generate_incremental_processing_job` - Incremental ETL jobs
- `generate_change_data_capture_job` - CDC processing jobs

### Batch Processing Tools (7 Tools)
- `batch_process_files` - Process multiple SQL files
- `batch_process_directory` - Process entire directories
- `extract_sql_from_pdf` - Extract SQL from PDFs
- `get_batch_status` - Job status monitoring
- `cancel_batch_job` - Job cancellation
- `get_active_batch_jobs` - Active job listing
- `get_recent_batch_jobs` - Job history

### Code Analysis & Pattern Detection Tools (6 Tools)
- `analyze_code_patterns` - Duplicate pattern detection
- `generate_utility_functions` - Utility function generation
- `refactor_code_with_patterns` - Automated refactoring
- `get_stored_patterns` - Pattern database access
- `search_code_patterns` - Pattern search functionality
- `get_pattern_statistics` - Pattern analytics

### ⚡ Advanced Optimization Tools (6 Tools)
- `analyze_data_flow` - Data flow analysis
- `suggest_partitioning_strategy` - Partitioning optimization
- `recommend_join_strategy` - Join optimization
- `estimate_performance_impact` - Performance impact estimation
- `generate_comprehensive_optimizations` - Complete optimization package
- `get_optimization_analytics` - Optimization effectiveness analytics

### 📈 Data Source Analysis Tools (3 Tools)
- `analyze_codebase` - Comprehensive codebase analysis
- `analyze_s3_data_source` - S3 data source analysis
- `analyze_delta_table` - Delta table analysis

### 💾 Memory & Context Management Tools (4 Tools)
- `store_context` - Context storage
- `get_context` - Context retrieval
- `get_conversion_history` - Conversion history access
- `search_conversions` - Conversion search functionality

## Usage Examples

### Flagship Complete Conversion (Recommended)
```python
# One-call complete workflow - SQL to optimized PySpark + Glue job
result = complete_sql_conversion(
    sql_content="SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    optimization_level="aggressive",
    include_glue_template=True
)
# Returns: PySpark code + Glue job + optimizations + performance analysis
```

### Basic SQL Conversion
```python
# Core conversion only
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
