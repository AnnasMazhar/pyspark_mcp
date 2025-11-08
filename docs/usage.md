# Usage Guide

## Quick Start

### 1. Basic SQL to PySpark Conversion

```python
# Using the MCP tool
convert_sql_to_pyspark(
    sql_query="SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    dialect="postgres"
)
```

### 2. Advanced Optimization Analysis

```python
# Analyze existing PySpark code
analyze_data_flow(
    pyspark_code="""
    df1 = spark.table('large_table')
    df2 = spark.table('small_table') 
    result = df1.join(df2, 'key')
    """,
    table_info={"large_table": {"size_mb": 5000}, "small_table": {"size_mb": 100}}
)
```

### 3. Batch Processing

```python
# Process multiple SQL files
batch_process_directory(
    directory_path="/path/to/sql/files",
    output_dir="/path/to/output"
)
```

## Common Workflows

### Migration from SQL Database

1. **Extract SQL queries** from existing database or documentation
2. **Convert to PySpark** using multi-dialect support
3. **Apply optimizations** based on recommendations
4. **Test and validate** the converted code

### Performance Optimization

1. **Analyze data flow** in existing PySpark code
2. **Get optimization recommendations** with impact estimates
3. **Apply high-priority optimizations** first
4. **Monitor performance improvements** through analytics

### Large-Scale Batch Migration

1. **Organize SQL files** in directories by database/schema
2. **Use batch processing** for concurrent conversion
3. **Review conversion results** and handle failures
4. **Apply consistent optimizations** across all converted code

## Examples

See the `examples/` directory for comprehensive demonstrations of all features.