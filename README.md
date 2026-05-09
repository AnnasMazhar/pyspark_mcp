# PySpark MCP Server

<!-- mcp-name: io.github.AnnasMazhar/pyspark-mcp -->

SQL migration assistance, AWS Glue job generation, and Spark code optimization — as an MCP server.

[![CI Pipeline](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What It Does

- **SQL Dialect Transpilation** — Convert between PostgreSQL, Oracle, Redshift, MySQL, Snowflake, and Spark SQL using [SQLGlot](https://github.com/tobymao/sqlglot)
- **PySpark DataFrame API Generation** — Generate DataFrame API code from SQL with optimization hints
- **AWS Glue Integration** — Job templates, DynamicFrame conversions, Data Catalog definitions, S3 optimization strategies
- **Batch Processing** — Process hundreds of SQL files concurrently
- **Code Review & Optimization** — Analyze existing PySpark code for performance improvements
- **Pattern Detection** — Find code duplication and suggest refactoring

## What It Doesn't Do

- Recursive CTEs → provides Spark SQL equivalent + guidance (PySpark has no native recursive CTE support)
- MERGE/PIVOT/CONNECT BY → transpiles to Spark SQL, provides DataFrame API guidance
- Perfect 1:1 DataFrame API transpilation for all SQL — complex queries get Spark SQL + optimization recommendations

## Quick Start

```bash
pip install -e .
pyspark-mcp  # starts the MCP server
```

## MCP Configuration

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pyspark": {
      "command": "pyspark-mcp",
      "args": []
    }
  }
}
```

### Hermes Agent

Add to `~/.hermes/config.yaml`:

```yaml
mcp:
  servers:
    pyspark:
      command: pyspark-mcp
      enabled_tools: all
```

### Docker

```bash
docker compose up -d
```

## Tools

### SQL Conversion
- `convert_sql_to_pyspark` — Convert SQL to PySpark with dialect detection
- `analyze_sql_context` — Analyze SQL complexity and suggest approach

### AWS Glue
- `generate_aws_glue_job_template` — Generate complete Glue job scripts
- `convert_dataframe_to_dynamic_frame` — DataFrame ↔ DynamicFrame conversion
- `generate_data_catalog_table_definition` — Data Catalog table definitions
- `generate_incremental_processing_job` — Incremental/CDC job generation
- `analyze_s3_optimization_opportunities` — S3 layout and partitioning analysis

### Optimization
- `review_pyspark_code` — Code review with performance recommendations
- `optimize_pyspark_code` — Suggest optimizations for existing code
- `recommend_join_strategy` — Broadcast vs shuffle join recommendations
- `suggest_partitioning_strategy` — Partitioning recommendations

### Batch Processing
- `batch_process_files` — Process multiple SQL files concurrently
- `batch_process_directory` — Convert entire directories

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Test
pytest tests/ -v --cov=pyspark_tools

# Format
black pyspark_tools tests
isort pyspark_tools tests

# Lint
flake8 pyspark_tools tests
```

## Architecture

```
pyspark_tools/
├── server.py              # FastMCP server + tool definitions
├── sql_converter.py       # SQLGlot-based transpilation + DataFrame API generation
├── aws_glue_integration.py # Glue job templates, DynamicFrame, Data Catalog
├── advanced_optimizer.py  # Performance analysis + optimization suggestions
├── batch_processor.py     # Concurrent file processing
├── code_reviewer.py       # PySpark code review patterns
├── duplicate_detector.py  # Code deduplication
├── data_source_analyzer.py # Data source analysis
└── file_utils.py          # File I/O utilities
```

## CI/CD

- ✅ 256 tests passing
- ✅ 71% code coverage
- ✅ Code quality checks (black, isort, flake8)
- ✅ Python 3.11 tested

## License

MIT — see [LICENSE](LICENSE).
