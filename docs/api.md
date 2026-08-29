# API Reference

Fourteen FastMCP tools. Each is a router with a `mode` parameter. Helpers
in `pyspark_tools.server` are **not** MCP tools unless listed here.

## `convert`

SQL conversion, batch files, directory walk, PDF extraction.

```python
convert(mode="sql", sql_query="SELECT id FROM users", dialect="postgres")
```

Modes: `sql`, `batch_files`, `batch_dir`, `from_pdf`.

## `analyze`

SQL context, data-flow, codebase directory, workspace files.

```python
analyze(mode="sql_context", sql_content="SELECT * FROM orders")
```

Modes: `sql_context`, `data_flow`, `codebase`, `workspace`.

## `optimize`

Pattern-based suggestions for PySpark source, join strategy, partitioning.
`optimize(mode="code")` does **not** rewrite the input.

```python
optimize(mode="code", code="df.join(other, 'id')", optimization_level="standard")
```

Modes: `code`, `joins`, `partitioning`, `comprehensive`.

Recommendations are qualitative (`high` / `medium` / `low` potential). This
package does not measure runtime.

## `review`

Code review, pattern analysis, duplicate detection.

```python
review(mode="code", code="df.collect()")
```

Modes: `code`, `patterns`, `duplicates`.

## `glue_job`

Glue job template strings, DynamicFrame conversion, job properties.

```python
glue_job(mode="template", job_name="orders_etl", sql_query="SELECT * FROM orders")
```

Modes: `template`, `dynamic_frame`, `properties`, `sql_conversion`.

Emits source text. Does not create a Glue job in AWS.

## `glue_schema`

Sample-data schema detect, evolution strategy, Data Catalog table definition.

```python
glue_schema(mode="detect", sample_data=[{"id": 1}], table_name="orders")
```

Modes: `detect`, `evolve`, `catalog`.

## `glue_s3`

Path-heuristic S3 layout suggestions. **No AWS API call**; figures are not measured.

```python
glue_s3(mode="analyze", s3_location="s3://bucket/path", database_name="raw", table_name="orders")
```

Modes: `analyze`, `optimize`, `consolidate`.

## `glue_data`

Incremental / CDC / bookmark configuration templates.

```python
glue_data(mode="bookmarks", job_name="orders_etl")
```

Modes: `incremental`, `cdc`, `bookmarks`.

## `refactor`

Pattern-based refactor, utility extraction, pipeline / project structure.

```python
refactor(mode="utilities", code_samples=["df.filter(col('a')==1)", "df.filter(col('b')==2)"])
```

Modes: `patterns`, `utilities`, `pipeline`.

## `search`

Search stored conversions, patterns, or context keys.

```python
search(mode="conversions", query="orders", limit=10)
```

Modes: `conversions`, `patterns`, `context`.

## `context`

Store/get conversion context; real-time SQL assistance.

```python
context(mode="store", conversion_id="job-1", context_data={"dialect": "postgres"})
```

Modes: `store`, `get`, `assist`.

## `batch_status`

Batch job status, cancel, active list, recent list.

```python
batch_status(mode="recent", limit=10)
```

Modes: `status`, `cancel`, `active`, `recent`.

## `s3_source`

Analyze an S3 prefix or Delta table path. Uses host AWS credentials when
`boto3` is installed (`pip install 'pyspark-tools[aws]'`).

```python
s3_source(mode="analyze", s3_path="s3://bucket/prefix")
```

Modes: `analyze`, `delta`.

## `analytics`

Optimization-effectiveness and conversion-history stats from the local SQLite store.

```python
analytics(mode="usage", limit=20)
```

Modes: `optimization`, `usage`.
