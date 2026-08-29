# PySpark MCP Server

SQL migration assistance, AWS Glue job *template* generation, and Spark code
optimization — as an MCP server.

[![CI Pipeline](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/pr-validation.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What It Does

- **SQL Dialect Transpilation** — Convert between PostgreSQL, Oracle, Redshift, MySQL, Snowflake, and Spark SQL using [SQLGlot](https://github.com/tobymao/sqlglot)
- **PySpark DataFrame API Generation** — Generate DataFrame API *source text* from SQL, with optimization hints
- **AWS Glue templates** — Job script strings, DynamicFrame conversions, Data Catalog definitions, S3 layout advice
- **Batch Processing** — Walk SQL files/directories and emit converted modules
- **Code Review & Optimization** — Pattern-based review of existing PySpark source
- **Pattern Detection** — Find duplicated snippets and suggest utilities

## What It Doesn't Do

- Recursive CTEs → provides Spark SQL equivalent + guidance (PySpark has no native recursive CTE support)
- MERGE/PIVOT/CONNECT BY → transpiles to Spark SQL, provides DataFrame API guidance
- Perfect 1:1 DataFrame API transpilation for all SQL — complex queries get Spark SQL + recommendations
- It does **not** start a SparkSession, submit Glue jobs, or execute SQL

## Why this vs calling sqlglot yourself

SQLGlot already transpiles dialects. This MCP adds three things around that kernel: DataFrame-API pretty-printing with join/window/cast mappings that the conversion tests lock, Glue job *boilerplate strings* (bookmarks, DynamicFrames, catalog tables) so an agent can emit a file instead of assembling one, and a 14-tool FastMCP surface so an LLM picks `convert` / `mode=sql` instead of wiring sqlglot itself. If you only need `sqlglot.transpile(...)`, use sqlglot.

## Quick Start

```bash
pip install -e .
pyspark-mcp  # THE entry point (pyspark_tools:main)
```

`run_server.py` is a development convenience that inserts `sys.path` and prints startup banners. Prefer `pyspark-mcp` in configs and production.

## Example: SQL → PySpark

```sql
SELECT o.customer_id, c.name, SUM(o.amount) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'paid'
GROUP BY o.customer_id, c.name
```

Call `convert` with `mode=sql`. Typical generated DataFrame API:

```python
from pyspark.sql.functions import col, sum as spark_sum

orders_df = spark.table("orders").alias("o")
customers_df = spark.table("customers").alias("c")
result_df = (
    orders_df.join(customers_df, col("o.customer_id") == col("c.id"), "inner")
    .filter(col("o.status") == "paid")
    .groupBy(col("o.customer_id"), col("c.name"))
    .agg(spark_sum(col("o.amount")).alias("total"))
)
```

Exact output depends on dialect detection and fallbacks; conversion tests in `tests/test_sql_conversion_fixes.py` pin the important constructs.

## MCP Configuration

### Claude Desktop

macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

Linux: `~/.config/Claude/claude_desktop_config.json`

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

Fourteen routers. Each takes `mode=` plus a small set of fields. Old 51-tool names are **not** registered MCP tools (they remain as Python helpers in `server.py`).

### `convert` — SQL → PySpark, batch files, PDF
```python
convert(mode="sql", sql_query="SELECT id FROM users", dialect="postgres")
convert(mode="batch_files", file_paths=["etl/job.sql"], output_dir="out")
```

### `analyze` — context, data flow, codebase, workspace
```python
analyze(mode="sql_context", sql_content="SELECT * FROM orders o JOIN items i ON o.id = i.order_id")
```

### `optimize` — code, joins, partitioning, comprehensive
```python
optimize(mode="code", code="df.join(other, 'id').select('*')", optimization_level="standard")
```

### `review` — code review, patterns, duplicates
```python
review(mode="code", code="df = spark.table('t')\ndf.collect()")
```

### `glue_job` — template, DynamicFrame, properties, SQL conversion
```python
glue_job(mode="template", job_name="orders_etl", sql_query="SELECT * FROM orders")
```

### `glue_schema` — detect, evolve, catalog
```python
glue_schema(mode="detect", sample_data=[{"id": 1}], table_name="orders")
```

### `glue_s3` — analyze, optimize, consolidate
```python
glue_s3(mode="analyze", s3_location="s3://bucket/path", database_name="raw", table_name="orders")
```

### `glue_data` — incremental, CDC, bookmarks
```python
glue_data(mode="bookmarks", job_name="orders_etl")
```

### `refactor` — patterns, utilities, pipeline
```python
refactor(mode="utilities", code_samples=["df.filter(col('a')==1)", "df.filter(col('b')==2)"])
```

### `search` — conversions, patterns, context
```python
search(mode="conversions", query="orders", limit=10)
```

### `context` — store, get, assist
```python
context(mode="store", conversion_id="job-1", context_data={"dialect": "postgres"})
```

### `batch_status` — status, cancel, active, recent
```python
batch_status(mode="recent", limit=10)
```

### `s3_source` — analyze S3 / Delta (uses host AWS credentials if boto3 is installed)
```python
s3_source(mode="analyze", s3_path="s3://bucket/prefix")
```

### `analytics` — optimization / usage stats
```python
analytics(mode="usage", limit=20)
```

## Security

This MCP can **read local files** (SQL, TXT, PDF) and, if the `[aws]` extra is installed, **list/read S3 with the host's default AWS credentials**. File tools only allow paths under the process working directory (or an explicit `base_path` / `FileHandler(base_directory=...)`). That is not a sandbox.

Run the server under a restricted OS account. Do not point it at secrets directories. Do not attach AWS credentials with write access unless you intend S3 reads via `s3_source` / `glue_s3`. Optional extras:

```bash
pip install -e ".[aws]"    # boto3 for S3/Glue catalog helpers
pip install -e ".[spark]"  # pyspark — not required at runtime; generated code only
```

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

Requires **Python 3.11+** (matches the CI matrix).

## Architecture

```
pyspark_tools/
├── server.py              # FastMCP server + helper implementations
├── consolidated_tools.py  # 14 @app.tool() routers
├── sql_converter.py       # SQLGlot-based transpilation + DataFrame API generation
├── aws_glue_integration.py # Glue job templates, DynamicFrame, Data Catalog
├── advanced_optimizer.py  # Performance analysis + optimization suggestions
├── batch_processor.py     # Concurrent file processing
├── code_reviewer.py       # PySpark code review patterns
├── duplicate_detector.py  # Code deduplication
├── data_source_analyzer.py # Data source analysis (optional boto3)
└── file_utils.py          # File I/O with allow-root checks
```

## License

MIT — see [LICENSE](LICENSE).

---
`mcp-name: io.github.AnnasMazhar/pyspark-mcp`
