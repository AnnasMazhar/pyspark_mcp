# API Reference

## MCP Tool Endpoints

### Core Conversion Tools

#### `convert_sql_to_pyspark`
Convert SQL queries to optimized PySpark code.

**Parameters:**
- `sql_query` (str): The SQL query to convert
- `dialect` (str, optional): Source SQL dialect (postgres, oracle, redshift, spark)
- `table_info` (dict, optional): Table metadata for optimization
- `store_result` (bool): Whether to store conversion in history

**Returns:**
- `status`: "success" or "error"
- `pyspark_code`: Generated PySpark code
- `optimizations`: List of optimization suggestions
- `dialect_used`: Detected or specified dialect
- `warnings`: Any conversion warnings

#### `review_pyspark_code`
Review PySpark code for best practices and AWS Glue compatibility.

**Parameters:**
- `code` (str): PySpark code to review
- `focus_areas` (list, optional): Areas to focus on ["aws_glue", "performance", "best_practice", "style"]

**Returns:**
- `issues`: List of identified issues with severity levels
- `metrics`: Code quality metrics
- `recommendations`: Actionable improvement suggestions

### Advanced Optimization Tools

#### `analyze_data_flow`
Analyze data flow patterns in PySpark code.

**Parameters:**
- `pyspark_code` (str): PySpark code to analyze
- `table_info` (dict, optional): Table metadata

**Returns:**
- `analysis`: Comprehensive data flow analysis including nodes, joins, aggregations
- `estimated_cost`: Execution cost estimate

#### `suggest_partitioning_strategy`
Suggest optimal partitioning strategies based on query patterns.

**Parameters:**
- `pyspark_code` (str): PySpark code to analyze
- `table_info` (dict, optional): Table metadata

**Returns:**
- `strategies`: List of partitioning recommendations with performance impact

#### `recommend_join_strategy`
Recommend specific join strategies based on estimated table sizes.

**Parameters:**
- `pyspark_code` (str): PySpark code to analyze
- `table_info` (dict, optional): Table metadata

**Returns:**
- `optimizations`: Join optimization recommendations (broadcast vs shuffle)

#### `generate_comprehensive_optimizations`
Generate comprehensive optimization recommendations.

**Parameters:**
- `pyspark_code` (str): PySpark code to optimize
- `table_info` (dict, optional): Table metadata

**Returns:**
- `recommendations`: Complete list of optimization suggestions with priorities
- `estimated_total_improvement`: Combined performance improvement estimate

### Batch Processing Tools

#### `batch_process_files`
Process multiple SQL files in batch.

**Parameters:**
- `file_paths` (list): List of SQL file paths
- `output_dir` (str, optional): Output directory
- `job_name` (str, optional): Name for the batch job

**Returns:**
- `job_id`: Batch job identifier
- `status`: Processing status
- `results`: Processing results and statistics

#### `batch_process_directory`
Process all SQL files in a directory.

**Parameters:**
- `directory_path` (str): Directory containing SQL files
- `output_dir` (str, optional): Output directory
- `recursive` (bool): Whether to search subdirectories

**Returns:**
- `job_id`: Batch job identifier
- `total_files`: Number of files found
- `processing_status`: Current processing status

### Analytics Tools

#### `get_optimization_analytics`
Get analytics on optimization effectiveness and usage patterns.

**Parameters:**
- `optimization_type` (str, optional): Filter by optimization type
- `limit` (int): Maximum number of metrics to return

**Returns:**
- `metrics`: Historical optimization metrics
- `effectiveness`: Optimization effectiveness statistics

## Data Structures

### Table Info Format
```json
{
  "table_name": {
    "size_mb": 1000,
    "row_count": 1000000,
    "partitions": ["year", "month"],
    "file_format": "parquet"
  }
}
```

### Optimization Recommendation Format
```json
{
  "optimization_id": "abc123",
  "optimization_type": "broadcast_join",
  "title": "Use broadcast join for small table",
  "description": "Table is small enough for broadcast join optimization",
  "code_changes": ["broadcast(small_df)"],
  "priority": "high",
  "performance_estimate": {
    "estimated_improvement": 0.4,
    "confidence_level": 0.8,
    "implementation_complexity": "low"
  }
}
```