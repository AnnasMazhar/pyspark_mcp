# PySpark MCP Server

SQL migration assistance, AWS Glue job *template* generation, and Spark code
optimization — as an MCP server.

> **Not the live-Spark `pyspark-mcp` package.** This project is SQL → PySpark /
> Glue *source generation*, published as [`pyspark-tools`](https://pypi.org/project/pyspark-tools/).
> [SemyonSinchenko/pyspark-mcp](https://pypi.org/project/pyspark-mcp/) introspects a
> running SparkSession. A deprecated `pyspark-mcp` console script remains here so
> old configs keep working; it prints a warning, then starts this server.

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
- `optimize(mode="code")` returns **suggestions**; it does not rewrite your code
- `glue_s3` is a **path heuristic** (no AWS call, no measured speedups)
- It does **not** replace [SemyonSinchenko/pyspark-mcp](https://pypi.org/project/pyspark-mcp/) for live catalog/plans

## Why this vs calling sqlglot yourself

SQLGlot already transpiles dialects. This MCP adds three things around that kernel: DataFrame-API pretty-printing with join/window/cast mappings that the conversion tests lock, Glue job *boilerplate strings* (bookmarks, DynamicFrames, catalog tables) so an agent can emit a file instead of assembling one, and a 14-tool FastMCP surface so an LLM picks `convert` / `mode=sql` instead of wiring sqlglot itself. If you only need `sqlglot.transpile(...)`, use sqlglot.

## Quick Start

```bash
pip install pyspark-tools
pyspark-tools
```

Zero-clone alternative: `uvx pyspark-tools`. `run_server.py` is a development convenience that inserts `sys.path` and prints startup banners. Prefer `pyspark-tools` in configs and production.

## Try it

```bash
pip install pyspark-tools
python -c "from pathlib import Path; from pyspark_tools.sql_converter import SQLToPySparkConverter as C; from pyspark_tools.consolidated_tools import glue_job; c,s,o=C(),Path('examples'),Path('examples/out'); [(o/f'{n}.py').write_text(c.convert_sql_to_pyspark((s/f'{n}.sql').read_text(), dialect=d).pyspark_code) for n,d in [('postgres_orders','postgres'),('oracle_decode','oracle')]]; (o/'orders_etl_glue.py').write_text(glue_job(mode='template', job_name='orders_etl', sql_query=(s/'postgres_orders.sql').read_text())['template'])"
```

Writes the same files as `examples/out/`. MCP stdio CLI: `pyspark-tools`.

## Example: SQL → PySpark

```sql
SELECT o.customer_id, c.name, SUM(o.amount) AS total
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'paid'
GROUP BY o.customer_id, c.name
```

Call `convert` with `mode=sql`. Captured converter output (`dialect=spark`):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg, min, max, countDistinct,
    coalesce, concat, datediff, date_add, to_date,
    row_number, rank, lag, lead,
)
from pyspark.sql.window import Window

# Generated from SPARK SQL
spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()

# Load table: customers
customers_df = spark.table('customers')
# Load table: orders
orders_df = spark.table('orders')

# Main query
result_df = (orders_df.alias('o')
    .join(customers_df.alias('c'), (col('o.customer_id') == col('c.id')), 'inner')
    .filter((col('o.status') == lit('paid')))
    .groupBy(col('o.customer_id'), col('c.name'))
    .select(col('o.customer_id'), col('c.name'), (spark_sum(col('o.amount'))).alias('total')))
```

Exact output depends on dialect detection and fallbacks; conversion tests in `tests/test_sql_conversion_fixes.py` pin the important constructs. Notebook-style `import *` / `show()` is opt-in via `style="notebook"` on the converter.

## MCP Configuration

### Claude Desktop

macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

Linux: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "pyspark": {
      "command": "pyspark-tools",
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
      command: pyspark-tools
      enabled_tools: all
```

### Docker

The image is **stdio only** (FastMCP over stdin/stdout). There is no HTTP server
on port 8000. `docker compose up` is for local tests, not a health-checkable
web service.

```bash
docker compose --profile test run --rm pyspark-tools-test
```

## Tools

Three primary tools. The other eleven routers stay registered this minor
version but are **deprecated** — prefer `convert`, `glue_job`, and `review`.

### `convert` — SQL → PySpark (including `mode=batch_dir`)
```python
convert(mode="sql", sql_query="SELECT id FROM users", dialect="postgres")
convert(mode="batch_dir", directory_path="etl/", output_dir="out")
```

### `glue_job` — Glue 5.0 job *template* strings
```python
glue_job(mode="template", job_name="orders_etl", sql_query="SELECT * FROM orders")
```

### `review` — code review, patterns, duplicates
```python
review(mode="code", code="df = spark.table('t')\ndf.collect()")
```

**Legacy / deprecated:** `analyze`, `optimize`, `glue_schema`, `glue_s3`,
`glue_data`, `refactor`, `search`, `context`, `batch_status`, `s3_source`,
`analytics`. Still callable; do not advertise to new agents.

## Security

This MCP can **read local files** (SQL, TXT, PDF) and, if the `[aws]` extra is installed, **list/read S3 with the host's default AWS credentials**. File tools only allow paths under the process working directory (or an explicit `base_path` / `FileHandler(base_directory=...)`). That is not a sandbox.

Run the server under a restricted OS account. Do not point it at secrets directories. Do not attach AWS credentials with write access unless you intend S3 reads via `s3_source` / `glue_s3`. Optional extras:

```bash
pip install "pyspark-tools[aws]"    # boto3 for S3/Glue catalog helpers
pip install "pyspark-tools[spark]"  # pyspark — not required at runtime; generated code only
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
