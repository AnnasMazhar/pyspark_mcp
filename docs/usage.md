# Usage Guide

## Quick Start

Install from PyPI, then call the 14 `mode=` routers (not the old 51 helper names).

```bash
pip install pyspark-tools
```

### 1. Basic SQL to PySpark conversion

```python
from pyspark_tools.consolidated_tools import convert

convert(
    mode="sql",
    sql_query="SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    dialect="postgres",
)
```

### 2. Pattern-based review (does not rewrite)

```python
from pyspark_tools.consolidated_tools import optimize, review

optimize(
    mode="code",
    code="df1 = spark.table('large_table')\ndf2 = spark.table('small_table')\nresult = df1.join(df2, 'key')",
)
review(mode="code", code="df = spark.table('t')\ndf.collect()")
```

### 3. Batch processing

```python
from pyspark_tools.consolidated_tools import convert

convert(
    mode="batch_dir",
    directory_path="/path/to/sql/files",
    output_dir="/path/to/output",
)
```

## Common Workflows

### Migration from SQL Database

1. **Extract SQL queries** from existing database or documentation
2. **Convert to PySpark** with `convert(mode="sql", dialect=...)`
3. **Review suggestions** from `optimize` / `review` (pattern-based, not measured)
4. **Paste and validate** the generated source in your Spark/Glue job

### Performance Optimization

1. **Review** existing PySpark with `review(mode="code")`
2. **Collect suggestions** with `optimize(mode="code")` — this does not rewrite
3. Apply high-priority suggestions yourself
4. Measure in Spark UI / Glue metrics, not from this MCP

### Large-Scale Batch Migration

1. Organize SQL files in directories by database/schema
2. Use `convert(mode="batch_dir")` for concurrent conversion
3. Review conversion results and handle `fallback_used` / `status=error`
4. Apply consistent review across converted modules

## Glue templates

```python
from pyspark_tools.consolidated_tools import glue_job

glue_job(mode="template", job_name="orders_etl", sql_query="SELECT * FROM orders")
```

Default Glue version is **5.0**. Bookmarks require DynamicFrame reads with
`transformation_ctx`; `spark.sql` / DataFrame reads do not bookmark.

See [API Reference](api.md) for every router and mode.
