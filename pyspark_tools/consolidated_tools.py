"""
Consolidated tool definitions for PySpark Tools MCP Server.

Reduces 51 individual ``@app.tool()`` functions to 14 consolidated tools
using a ``mode`` parameter pattern. Each consolidated tool dispatches to
the appropriate internal helper from ``server.py``.

Usage via MCP client::

    # Instead of 51 individual tools:
    result = await client.call_tool("convert_sql_to_pyspark", {...})
    result = await client.call_tool("batch_process_files", {...})

    # Use 14 consolidated tools:
    result = await client.call_tool("convert", {"mode": "sql", "sql_query": "..."})
    result = await client.call_tool("convert", {"mode": "batch_files", "file_paths": [...]})

Import helpers lazily inside each function body to avoid circular imports —
``server.py`` imports this module at its bottom after all helpers are defined.
"""

import json
from typing import Any, Dict, List, Optional

from pyspark_tools.server import app


# ===========================================================================
# 1. CONVERT — SQL conversion, batch processing, PDF extraction
# ===========================================================================
# Modes:
#   sql         - Convert SQL query to PySpark code
#   batch_files - Batch-process multiple SQL files
#   batch_dir   - Batch-process a directory of SQL files
#   from_pdf    - Extract SQL from a PDF file and convert
# ===========================================================================
@app.tool()
def convert(
    mode: str,
    # --- mode='sql' parameters ---
    sql_query: Optional[str] = None,
    table_info: Optional[Dict] = None,
    dialect: Optional[str] = None,
    optimization_level: str = "standard",
    include_glue_template: bool = False,
    # --- mode='batch_files' parameters ---
    file_paths: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
    job_name: Optional[str] = None,
    # --- mode='batch_dir' parameters ---
    directory_path: Optional[str] = None,
    recursive: bool = True,
    # --- mode='from_pdf' parameters ---
    pdf_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert SQL to PySpark code or process SQL files in batch.

    **Modes:**

    ``sql``
        Convert a single SQL query to PySpark code.
        *Parameters:* ``sql_query`` (required), ``table_info``, ``dialect``,
        ``optimization_level``, ``include_glue_template``

    ``batch_files``
        Process multiple SQL files into PySpark.
        *Parameters:* ``file_paths`` (required list), ``output_dir``, ``job_name``

    ``batch_dir``
        Process all SQL files in a directory.
        *Parameters:* ``directory_path`` (required), ``output_dir``, ``recursive``,
        ``job_name``

    ``from_pdf``
        Extract SQL from a PDF file and convert to PySpark.
        *Parameters:* ``pdf_path`` (required)
    """
    # Lazy import to avoid circular dependency — server.py is fully loaded by now
    from pyspark_tools import server as _s

    if mode == "sql":
        if not sql_query:
            return {"status": "error", "message": "sql_query is required for mode='sql'"}

        if include_glue_template:
            return _s.complete_sql_conversion(
                sql_query=sql_query,
                table_info=table_info,
                dialect=dialect,
                optimization_level=optimization_level,
                include_glue_template=True,
            )
        return _s.convert_sql_to_pyspark(
            sql_query=sql_query,
            table_info=table_info,
            dialect=dialect,
        )

    elif mode == "batch_files":
        if not file_paths:
            return {"status": "error", "message": "file_paths is required for mode='batch_files'"}
        return _s.batch_process_files(
            file_paths=file_paths,
            output_dir=output_dir or ".",
            job_name=job_name,
        )

    elif mode == "batch_dir":
        if not directory_path:
            return {"status": "error", "message": "directory_path is required for mode='batch_dir'"}
        return _s.batch_process_directory(
            directory_path=directory_path,
            output_dir=output_dir or ".",
            recursive=recursive,
            job_name=job_name,
        )

    elif mode == "from_pdf":
        if not pdf_path:
            return {"status": "error", "message": "pdf_path is required for mode='from_pdf'"}
        return _s.extract_sql_from_pdf(pdf_path=pdf_path)

    else:
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: sql, batch_files, batch_dir, from_pdf"}


# ===========================================================================
# 2. ANALYZE — SQL context, data flow, codebase, workspace analysis
# ===========================================================================
# Modes:
#   sql_context - Analyze SQL query context (schemas, tables, dialect)
#   data_flow   - Analyze data flow patterns in PySpark code
#   codebase    - Analyze a PySpark codebase directory
#   workspace   - Analyze a workspace (SQL + project structure)
# ===========================================================================
@app.tool()
def analyze(
    mode: str,
    # --- mode='sql_context' ---
    sql_content: Optional[str] = None,
    selected_text: Optional[str] = None,
    # --- mode='data_flow' ---
    pyspark_code: Optional[str] = None,
    table_info: Optional[Dict] = None,
    # --- mode='codebase' ---
    directory_path: Optional[str] = None,
    include_optimization_suggestions: bool = True,
    scan_depth: int = 3,
    # --- mode='workspace' ---
    workspace_path: Optional[str] = None,
    include_project_structure: bool = True,
    workspace_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Analyze SQL or PySpark code for context, data flow, or optimization opportunities.

    **Modes:**

    ``sql_context``
        Analyze SQL context (schemas, tables, dialect, complexity).
        *Parameters:* ``sql_content`` or ``selected_text``

    ``data_flow``
        Analyze data flow patterns in PySpark code.
        *Parameters:* ``pyspark_code`` (required), ``table_info``

    ``codebase``
        Analyze a PySpark codebase directory for patterns and issues.
        *Parameters:* ``directory_path`` (required), ``include_optimization_suggestions``,
        ``scan_depth``

    ``workspace``
        Full workspace analysis including project structure.
        *Parameters:* ``sql_content`` or ``workspace_path``, ``include_project_structure``,
        ``workspace_name``
    """
    from pyspark_tools import server as _s

    if mode == "sql_context":
        # Dispatch to analyze_sql_context or process_editor_selection based on input
        if selected_text:
            return _s.process_editor_selection(selected_text=selected_text)
        if sql_content:
            return _s.analyze_sql_context(sql_content=sql_content)
        return {"status": "error", "message": "sql_content or selected_text required for mode='sql_context'"}

    elif mode == "data_flow":
        if not pyspark_code:
            return {"status": "error", "message": "pyspark_code is required for mode='data_flow'"}
        return _s.analyze_data_flow(pyspark_code=pyspark_code, table_info=table_info)

    elif mode == "codebase":
        if not directory_path:
            return {"status": "error", "message": "directory_path is required for mode='codebase'"}
        return _s.analyze_codebase(
            directory_path=directory_path,
            include_optimization_suggestions=include_optimization_suggestions,
            scan_depth=scan_depth,
        )

    elif mode == "workspace":
        content = sql_content or ""
        return _s.workspace_analysis(
            sql_content=content,
            workspace_path=workspace_path or "",
            include_project_structure=include_project_structure,
            workspace_name=workspace_name or "pyspark-workspace",
            output_dir=".",
        )

    else:
        valid = "sql_context, data_flow, codebase, workspace"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 3. OPTIMIZE — Code optimization, join strategy, partitioning, comprehensive
# ===========================================================================
# Modes:
#   code          - Optimize PySpark code with optimization level
#   joins         - Recommend join strategies based on data sizes
#   partitioning  - Suggest partitioning strategies
#   comprehensive - Generate comprehensive optimization recommendations
# ===========================================================================
@app.tool()
def optimize(
    mode: str,
    pyspark_code: Optional[str] = None,
    code: Optional[str] = None,
    table_info: Optional[Dict] = None,
    optimization_level: str = "standard",
) -> Dict[str, Any]:
    """
    Optimize PySpark code and recommend performance improvements.

    **Modes:**

    ``code``
        Apply optimizations to PySpark code.
        *Parameters:* ``code`` (required), ``optimization_level``

    ``joins``
        Recommend join strategies based on estimated table sizes.
        *Parameters:* ``pyspark_code`` (required), ``table_info``

    ``partitioning``
        Suggest optimal partitioning strategies.
        *Parameters:* ``pyspark_code`` (required), ``table_info``

    ``comprehensive``
        Generate comprehensive optimization recommendations + performance estimates.
        *Parameters:* ``pyspark_code`` (required), ``table_info``
    """
    from pyspark_tools import server as _s

    if mode == "code":
        target = code or pyspark_code
        if not target:
            return {"status": "error", "message": "code is required for mode='code'"}
        return _s.optimize_pyspark_code(code=target, optimization_level=optimization_level)

    elif mode == "joins":
        if not pyspark_code:
            return {"status": "error", "message": "pyspark_code is required for mode='joins'"}
        return _s.recommend_join_strategy(pyspark_code=pyspark_code, table_info=table_info)

    elif mode == "partitioning":
        if not pyspark_code:
            return {"status": "error", "message": "pyspark_code is required for mode='partitioning'"}
        return _s.suggest_partitioning_strategy(pyspark_code=pyspark_code, table_info=table_info)

    elif mode == "comprehensive":
        if not pyspark_code:
            return {"status": "error", "message": "pyspark_code is required for mode='comprehensive'"}
        # Both estimate and full recommendations
        result = _s.generate_comprehensive_optimizations(
            pyspark_code=pyspark_code, table_info=table_info
        )
        # Also include performance estimates
        if result.get("status") == "success":
            estimates = _s.estimate_performance_impact(
                pyspark_code=pyspark_code, table_info=table_info
            )
            result["performance_estimates"] = estimates.get("estimates", {})
        return result

    else:
        valid = "code, joins, partitioning, comprehensive"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 4. REVIEW — Code review, pattern analysis, duplicate detection
# ===========================================================================
# Modes:
#   code      - Review PySpark code for issues and best practices
#   patterns  - Analyze code patterns for refactoring opportunities
#   duplicates - Find duplicate code patterns
# ===========================================================================
@app.tool()
def review(
    mode: str,
    code: Optional[str] = None,
    code_samples: Optional[List[str]] = None,
    focus_areas: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Review PySpark code for issues, patterns, and refactoring opportunities.

    **Modes:**

    ``code``
        Review PySpark code for issues, best practices, and performance.
        *Parameters:* ``code`` (required), ``focus_areas``

    ``patterns``
        Analyze code samples to discover common patterns.
        *Parameters:* ``code_samples`` (required list)

    ``duplicates``
        Detect duplicate patterns across code samples.
        *Parameters:* ``code_samples`` (required list)
    """
    from pyspark_tools import server as _s

    if mode == "code":
        if not code:
            return {"status": "error", "message": "code is required for mode='code'"}
        return _s.review_pyspark_code(code=code, focus_areas=focus_areas)

    elif mode == "patterns":
        if not code_samples:
            return {"status": "error", "message": "code_samples is required for mode='patterns'"}
        return _s.analyze_code_patterns(code_samples=code_samples)

    elif mode == "duplicates":
        if not code_samples:
            return {"status": "error", "message": "code_samples is required for mode='duplicates'"}
        return _s.analyze_code_patterns(code_samples=code_samples)

    else:
        valid = "code, patterns, duplicates"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 5. GLUE_JOB — Glue job templates, dynamic frames, properties, SQL
# ===========================================================================
# Modes:
#   template       - Generate AWS Glue job template from SQL
#   dynamic_frame  - Convert PySpark code to DynamicFrame-based
#   properties     - Generate Glue job properties/config
#   sql_conversion - Generate Glue job with SQL→PySpark conversion
# ===========================================================================
@app.tool()
def glue_job(
    mode: str,
    # --- common ---
    sql_query: Optional[str] = None,
    job_name: Optional[str] = None,
    source_database: Optional[str] = None,
    source_table: Optional[str] = None,
    target_database: Optional[str] = None,
    target_table: Optional[str] = None,
    # --- dynamic_frame ---
    pyspark_code: Optional[str] = None,
    # --- properties ---
    job_type: str = "etl",
    worker_type: str = "G.1X",
    number_of_workers: int = 2,
    max_retries: int = 0,
    timeout: int = 2880,
    glue_version: str = "4.0",
    enable_continuous_logging: bool = True,
    enable_metrics: bool = True,
    enable_spark_ui: bool = True,
    # --- template/sql_conversion ---
    source_format: str = "parquet",
    target_format: str = "parquet",
    include_bookmarking: bool = True,
    # --- template ---
    output_dir: Optional[str] = None,
    template_type: str = "standard",
    script_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate and manage AWS Glue job configurations and templates.

    **Modes:**

    ``template``
        Generate a complete AWS Glue job template.
        *Parameters:* ``sql_query``, ``job_name``, ``source_database``, ``source_table``,
        ``target_database``, ``target_table``, ``output_dir``, ``source_format``,
        ``target_format``, ``include_bookmarking``, ``template_type``, ``script_name``

    ``dynamic_frame``
        Convert PySpark DataFrame code to use DynamicFrames.
        *Parameters:* ``pyspark_code`` (required), ``source_database``, ``source_table``,
        ``target_database``, ``target_table``

    ``properties``
        Generate Glue job properties for AWS CLI/SDK/Terraform.
        *Parameters:* ``job_name`` (required), ``job_type``, ``worker_type``,
        ``number_of_workers``, ``max_retries``, ``timeout``, ``glue_version``,
        ``enable_continuous_logging``, ``enable_metrics``, ``enable_spark_ui``

    ``sql_conversion``
        Generate a Glue job that includes SQL-to-PySpark conversion.
        *Parameters:* ``sql_query`` (required), ``job_name`` (required),
        ``source_database``, ``source_table``, ``target_database``, ``target_table``,
        ``source_format``, ``target_format``, ``include_bookmarking``
    """
    from pyspark_tools import server as _s

    if mode == "template":
        return _s.generate_aws_glue_job_template(
            sql_query=sql_query or "",
            job_name=job_name or "glue-job",
            source_database=source_database or "",
            source_table=source_table or "",
            target_database=target_database or "",
            target_table=target_table or "",
            output_dir=output_dir or ".",
            source_format=source_format,
            target_format=target_format,
            include_bookmarking=include_bookmarking,
            template_type=template_type,
            script_name=script_name,
        )

    elif mode == "dynamic_frame":
        if not pyspark_code:
            return {"status": "error", "message": "pyspark_code is required for mode='dynamic_frame'"}
        return _s.convert_dataframe_to_dynamic_frame(
            pyspark_code=pyspark_code,
            source_database=source_database or "",
            source_table=source_table or "",
            target_database=target_database or "",
            target_table=target_table or "",
        )

    elif mode == "properties":
        if not job_name:
            return {"status": "error", "message": "job_name is required for mode='properties'"}
        return _s.generate_glue_job_properties(
            job_name=job_name,
            job_type=job_type,
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            max_retries=max_retries,
            timeout=timeout,
            glue_version=glue_version,
            enable_continuous_logging=enable_continuous_logging,
            enable_metrics=enable_metrics,
            enable_spark_ui=enable_spark_ui,
        )

    elif mode == "sql_conversion":
        if not sql_query or not job_name:
            return {"status": "error", "message": "sql_query and job_name required for mode='sql_conversion'"}
        return _s.generate_glue_job_with_sql_conversion(
            sql_query=sql_query,
            job_name=job_name,
            source_database=source_database or "",
            source_table=source_table or "",
            target_database=target_database or "",
            target_table=target_table or "",
            source_format=source_format,
            target_format=target_format,
            include_bookmarking=include_bookmarking,
        )

    else:
        valid = "template, dynamic_frame, properties, sql_conversion"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 6. GLUE_SCHEMA — Detect, evolve, and catalog Glue schemas
# ===========================================================================
# Modes:
#   detect  - Detect schema from sample data
#   evolve  - Generate schema evolution strategy
#   catalog - Generate Data Catalog table definition
# ===========================================================================
@app.tool()
def glue_schema(
    mode: str,
    # --- detect ---
    sample_data: Optional[Any] = None,
    table_name: Optional[str] = None,
    infer_partitions: bool = True,
    # --- evolve ---
    current_columns: Optional[List[Dict[str, str]]] = None,
    new_columns: Optional[List[Dict[str, str]]] = None,
    merge_behavior: str = "merge",
    case_sensitive: bool = False,
    # --- catalog ---
    database_name: Optional[str] = None,
    s3_location: Optional[str] = None,
    data_format: str = "parquet",
    columns: Optional[List[Dict[str, str]]] = None,
    partition_keys: Optional[List[str]] = None,
    enable_schema_evolution: bool = True,
) -> Dict[str, Any]:
    """
    Manage Glue Data Catalog schemas — detect, evolve, and define.

    **Modes:**

    ``detect``
        Detect schema from sample data and generate table definition.
        *Parameters:* ``sample_data`` (required dict/list), ``table_name`` (required),
        ``infer_partitions``

    ``evolve``
        Generate schema evolution strategy for handling schema changes.
        *Parameters:* ``current_columns`` (required), ``new_columns`` (required),
        ``merge_behavior``, ``case_sensitive``

    ``catalog``
        Generate AWS Glue Data Catalog table definition.
        *Parameters:* ``database_name`` (required), ``table_name`` (required),
        ``s3_location`` (required), ``data_format``, ``columns``, ``partition_keys``,
        ``enable_schema_evolution``
    """
    from pyspark_tools import server as _s

    if mode == "detect":
        if sample_data is None or not table_name:
            return {"status": "error", "message": "sample_data and table_name required for mode='detect'"}
        return _s.detect_schema_from_sample_data(
            sample_data=sample_data,
            table_name=table_name,
            infer_partitions=infer_partitions,
        )

    elif mode == "evolve":
        if not current_columns or not new_columns:
            return {"status": "error", "message": "current_columns and new_columns required for mode='evolve'"}
        return _s.generate_schema_evolution_strategy(
            current_columns=current_columns,
            new_columns=new_columns,
            merge_behavior=merge_behavior,
            case_sensitive=case_sensitive,
        )

    elif mode == "catalog":
        if not database_name or not table_name or not s3_location:
            return {"status": "error", "message": "database_name, table_name, s3_location required for mode='catalog'"}
        return _s.generate_data_catalog_table_definition(
            database_name=database_name,
            table_name=table_name,
            s3_location=s3_location,
            data_format=data_format,
            columns=columns,
            partition_keys=partition_keys,
            enable_schema_evolution=enable_schema_evolution,
        )

    else:
        valid = "detect, evolve, catalog"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 7. GLUE_S3 — S3 analysis, optimization, consolidation
# ===========================================================================
# Modes:
#   analyze      - Analyze S3 data layout and optimization opportunities
#   optimize     - Generate comprehensive S3 optimization strategy
#   consolidate  - Generate Glue job for small files consolidation
# ===========================================================================
@app.tool()
def glue_s3(
    mode: str,
    # --- common ---
    s3_location: Optional[str] = None,
    database_name: Optional[str] = None,
    table_name: Optional[str] = None,
    data_format: str = "parquet",
    query_patterns: Optional[List[str]] = None,
    # --- analyze ---
    data_size_gb: Optional[float] = None,
    # --- optimize ---
    target_file_size_mb: int = 128,
    compression_type: str = "snappy",
    enable_small_file_optimization: bool = True,
    # --- consolidate ---
    source_database: Optional[str] = None,
    source_table: Optional[str] = None,
    target_database: Optional[str] = None,
    target_table: Optional[str] = None,
    consolidation_strategy: str = "coalesce",
) -> Dict[str, Any]:
    """
    Analyze and optimize S3 data layouts for AWS Glue.

    **Modes:**

    ``analyze``
        Analyze S3 data layout for optimization opportunities.
        *Parameters:* ``s3_location`` (required), ``database_name`` (required),
        ``table_name`` (required), ``data_format``, ``query_patterns``, ``data_size_gb``

    ``optimize``
        Generate comprehensive S3 optimization strategy.
        *Parameters:* ``database_name`` (required), ``table_name`` (required),
        ``s3_location`` (required), ``data_format``, ``target_file_size_mb``,
        ``compression_type``, ``enable_small_file_optimization``, ``query_patterns``

    ``consolidate``
        Generate Glue job for small files consolidation.
        *Parameters:* ``source_database`` (required), ``source_table`` (required),
        ``target_database`` (required), ``target_table`` (required),
        ``target_file_size_mb``, ``consolidation_strategy``
    """
    from pyspark_tools import server as _s

    if mode == "analyze":
        if not s3_location or not database_name or not table_name:
            return {"status": "error", "message": "s3_location, database_name, table_name required for mode='analyze'"}
        return _s.analyze_s3_optimization_opportunities(
            s3_location=s3_location,
            database_name=database_name,
            table_name=table_name,
            data_format=data_format,
            query_patterns=query_patterns,
            data_size_gb=data_size_gb,
        )

    elif mode == "optimize":
        if not database_name or not table_name or not s3_location:
            return {"status": "error", "message": "database_name, table_name, s3_location required for mode='optimize'"}
        return _s.generate_s3_optimization_strategy(
            database_name=database_name,
            table_name=table_name,
            s3_location=s3_location,
            data_format=data_format,
            target_file_size_mb=target_file_size_mb,
            compression_type=compression_type,
            enable_small_file_optimization=enable_small_file_optimization,
            query_patterns=query_patterns,
        )

    elif mode == "consolidate":
        if not source_database or not source_table or not target_database or not target_table:
            return {"status": "error", "message": "source_database, source_table, target_database, target_table required"}
        return _s.generate_small_files_consolidation_job(
            source_database=source_database,
            source_table=source_table,
            target_database=target_database,
            target_table=target_table,
            target_file_size_mb=target_file_size_mb,
            consolidation_strategy=consolidation_strategy,
        )

    else:
        valid = "analyze, optimize, consolidate"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 8. GLUE_DATA — Incremental, CDC, bookmarks
# ===========================================================================
# Modes:
#   incremental - Generate Glue job with incremental processing
#   cdc         - Generate Change Data Capture Glue job
#   bookmarks   - Generate job bookmark configuration
# ===========================================================================
@app.tool()
def glue_data(
    mode: str,
    # --- common ---
    source_database: Optional[str] = None,
    source_table: Optional[str] = None,
    target_database: Optional[str] = None,
    target_table: Optional[str] = None,
    # --- incremental ---
    incremental_column: Optional[str] = None,
    incremental_strategy: str = "timestamp",
    transformation_sql: Optional[str] = None,
    # --- cdc ---
    cdc_column: str = "last_modified",
    cdc_strategy: str = "upsert",
    primary_keys: Optional[List[str]] = None,
    # --- bookmarks ---
    job_name: Optional[str] = None,
    bookmark_strategy: str = "enable",
    transformation_context_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Generate AWS Glue data processing jobs — incremental, CDC, bookmarks.

    **Modes:**

    ``incremental``
        Generate Glue job with incremental processing and job bookmarking.
        *Parameters:* ``source_database`` (required), ``source_table`` (required),
        ``target_database`` (required), ``target_table`` (required),
        ``incremental_column`` (required), ``incremental_strategy``,
        ``transformation_sql``

    ``cdc``
        Generate Change Data Capture (CDC) Glue job.
        *Parameters:* ``source_database`` (required), ``source_table`` (required),
        ``target_database`` (required), ``target_table`` (required),
        ``cdc_column``, ``cdc_strategy``, ``primary_keys``

    ``bookmarks``
        Generate job bookmark configuration for Glue jobs.
        *Parameters:* ``job_name`` (required), ``bookmark_strategy``,
        ``transformation_context_keys``
    """
    from pyspark_tools import server as _s

    if mode == "incremental":
        if not all([source_database, source_table, target_database, target_table, incremental_column]):
            return {"status": "error", "message": "All source/target params + incremental_column required"}
        return _s.generate_incremental_processing_job(
            source_database=source_database,
            source_table=source_table,
            target_database=target_database,
            target_table=target_table,
            incremental_column=incremental_column,
            incremental_strategy=incremental_strategy,
            transformation_sql=transformation_sql,
        )

    elif mode == "cdc":
        if not all([source_database, source_table, target_database, target_table]):
            return {"status": "error", "message": "All source/target params required for mode='cdc'"}
        return _s.generate_change_data_capture_job(
            source_database=source_database,
            source_table=source_table,
            target_database=target_database,
            target_table=target_table,
            cdc_column=cdc_column,
            cdc_strategy=cdc_strategy,
            primary_keys=primary_keys,
        )

    elif mode == "bookmarks":
        if not job_name:
            return {"status": "error", "message": "job_name is required for mode='bookmarks'"}
        return _s.generate_job_bookmark_configuration(
            job_name=job_name,
            bookmark_strategy=bookmark_strategy,
            transformation_context_keys=transformation_context_keys,
        )

    else:
        valid = "incremental, cdc, bookmarks"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 9. REFACTOR — Pattern-based refactoring, utilities, pipeline generation
# ===========================================================================
# Modes:
#   patterns  - Refactor code by replacing duplicates with utilities
#   utilities - Extract utility functions from code patterns
#   pipeline  - Generate optimized data pipeline code
# ===========================================================================
@app.tool()
def refactor(
    mode: str,
    # --- patterns / utilities ---
    original_code: Optional[str] = None,
    code_samples: Optional[List[str]] = None,
    patterns: Optional[List[Any]] = None,
    # --- pipeline ---
    data_sources: Optional[List[Dict[str, str]]] = None,
    processing_requirements: Optional[str] = None,
    target_format: str = "delta",
    include_monitoring: bool = True,
    # --- pipeline (project structure) ---
    sql_content: Optional[str] = None,
    workspace_name: Optional[str] = None,
    workspace_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    include_glue_template: bool = True,
    dialect: Optional[str] = None,
    include_batch_processing: bool = False,
    include_visualization: bool = False,
) -> Dict[str, Any]:
    """
    Refactor PySpark code and generate pipeline structures.

    **Modes:**

    ``patterns``
        Refactor code by replacing duplicate patterns with utility function calls.
        *Parameters:* ``original_code`` (required), ``code_samples`` (required)

    ``utilities``
        Extract common utility functions from code patterns.
        *Parameters:* ``code_samples`` (required), ``patterns``

    ``pipeline``
        Generate optimized PySpark data pipeline code or project structure.
        *Parameters (pipeline):* ``data_sources`` (required list), ``processing_requirements``
        (required), ``target_format``, ``include_monitoring``
        *Parameters (project):* ``sql_content``, ``workspace_name``, ``workspace_path``,
        ``output_dir``, ``include_glue_template``, ``dialect``, ``include_batch_processing``,
        ``include_visualization``
    """
    from pyspark_tools import server as _s

    if mode == "patterns":
        if not original_code or not code_samples:
            return {"status": "error", "message": "original_code and code_samples required for mode='patterns'"}
        return _s.refactor_code_with_patterns(
            original_code=original_code,
            code_samples=code_samples,
        )

    elif mode == "utilities":
        if not code_samples:
            return {"status": "error", "message": "code_samples is required for mode='utilities'"}
        return _s.generate_utility_functions(
            code_samples=code_samples,
            patterns=patterns,
        )

    elif mode == "pipeline":
        if data_sources and processing_requirements:
            return _s.generate_optimized_pipeline(
                data_sources=data_sources,
                processing_requirements=processing_requirements,
                target_format=target_format,
                include_monitoring=include_monitoring,
            )
        else:
            # Fall back to project structure generation
            return _s.generate_project_structure(
                sql_content=sql_content or "",
                workspace_name=workspace_name or "pyspark-workspace",
                workspace_path=workspace_path or ".",
                output_dir=output_dir or ".",
                include_glue_template=include_glue_template,
                dialect=dialect,
                include_batch_processing=include_batch_processing,
                include_visualization=include_visualization,
            )

    else:
        valid = "patterns, utilities, pipeline"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 10. SEARCH — Search conversions, patterns, and context
# ===========================================================================
# Modes:
#   conversions - Search previously converted SQL queries
#   patterns    - Search stored code patterns
#   context     - Search stored conversion context
# ===========================================================================
@app.tool()
def search(
    mode: str,
    query: Optional[str] = None,
    limit: int = 10,
    # --- patterns ---
    min_usage_count: int = 1,
    # --- context ---
    conversion_id: Optional[str] = None,
    key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Search stored conversions, code patterns, and context data.

    **Modes:**

    ``conversions``
        Search previously converted SQL queries and history.
        *Parameters:* ``query``, ``limit``
        If ``query`` is empty, returns recent conversion history.

    ``patterns``
        Search stored code patterns by description or template.
        *Parameters:* ``query``, ``limit``, ``min_usage_count``
        If ``query`` is empty, returns all stored patterns with ``min_usage_count``.

    ``context``
        Retrieve stored conversion context.
        *Parameters:* ``conversion_id`` or ``key``
    """
    from pyspark_tools import server as _s

    if mode == "conversions":
        if query:
            return _s.search_conversions(query=query, limit=limit)
        return _s.get_conversion_history(limit=limit)

    elif mode == "patterns":
        if query:
            return _s.search_code_patterns(query=query, limit=limit)
        return _s.get_stored_patterns(min_usage_count=min_usage_count, limit=limit)

    elif mode == "context":
        if conversion_id:
            return _s.get_context(conversion_id=conversion_id)
        return {"status": "error", "message": "conversion_id required for mode='context'"}

    else:
        valid = "conversions, patterns, context"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 11. CONTEXT — Store, retrieve, and assist with SQL context
# ===========================================================================
# Modes:
#   store  - Store conversion context data
#   get    - Retrieve stored context by conversion_id
#   assist - Real-time SQL assistance while editing
# ===========================================================================
@app.tool()
def context(
    mode: str,
    conversion_id: Optional[str] = None,
    context_data: Optional[Any] = None,
    # --- assist ---
    sql_query: Optional[str] = None,
    selected_text: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Store, retrieve, and work with SQL/PySpark conversion context.

    **Modes:**

    ``store``
        Store additional context data for a conversion.
        *Parameters:* ``conversion_id`` (required), ``context_data`` (required)

    ``get``
        Retrieve stored context for a conversion.
        *Parameters:* ``conversion_id`` (required)

    ``assist``
        Real-time SQL assistance — analyze and convert as you edit.
        *Parameters:* ``sql_query`` or ``selected_text``
    """
    from pyspark_tools import server as _s

    if mode == "store":
        if not conversion_id or context_data is None:
            return {"status": "error", "message": "conversion_id and context_data required for mode='store'"}
        return _s.store_context(conversion_id=conversion_id, context_data=context_data)

    elif mode == "get":
        if not conversion_id:
            return {"status": "error", "message": "conversion_id required for mode='get'"}
        return _s.get_context(conversion_id=conversion_id)

    elif mode == "assist":
        if selected_text:
            return _s.process_editor_selection(selected_text=selected_text)
        if sql_query:
            return _s.realtime_sql_assistance(sql_query=sql_query)
        return {"status": "error", "message": "sql_query or selected_text required for mode='assist'"}

    else:
        valid = "store, get, assist"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 12. BATCH_STATUS — Monitor and manage batch jobs
# ===========================================================================
# Modes:
#   status - Get status of a specific batch job
#   cancel - Cancel a running batch job
#   active - List all active batch jobs
#   recent - List recent batch jobs
# ===========================================================================
@app.tool()
def batch_status(
    mode: str,
    job_id: Optional[str] = None,
    limit: int = 20,
    status: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Monitor and manage batch processing jobs.

    **Modes:**

    ``status``
        Get the status of a specific batch job.
        *Parameters:* ``job_id`` (required)

    ``cancel``
        Cancel a running batch job.
        *Parameters:* ``job_id`` (required)

    ``active``
        List all currently active batch jobs.
        *Parameters:* none

    ``recent``
        List recent batch jobs.
        *Parameters:* ``limit``, ``status``
    """
    from pyspark_tools import server as _s

    if mode == "status":
        if not job_id:
            return {"status": "error", "message": "job_id is required for mode='status'"}
        return _s.get_batch_status(job_id=job_id)

    elif mode == "cancel":
        if not job_id:
            return {"status": "error", "message": "job_id is required for mode='cancel'"}
        return _s.cancel_batch_job(job_id=job_id)

    elif mode == "active":
        return _s.get_active_batch_jobs()

    elif mode == "recent":
        return _s.get_recent_batch_jobs(limit=limit, status=status)

    else:
        valid = "status, cancel, active, recent"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 13. S3_SOURCE — Analyze S3 data sources and Delta tables
# ===========================================================================
# Modes:
#   analyze - Analyze S3 data source (structure, format, optimization)
#   delta   - Analyze Delta table (properties, history, optimization)
# ===========================================================================
@app.tool()
def s3_source(
    mode: str,
    s3_path: Optional[str] = None,
    table_path: Optional[str] = None,
    include_schema_inference: bool = True,
    analyze_history: bool = False,
) -> Dict[str, Any]:
    """
    Analyze S3 data sources and Delta tables.

    **Modes:**

    ``analyze``
        Analyze S3 data source structure, format, and optimization opportunities.
        *Parameters:* ``s3_path`` (required), ``include_schema_inference``

    ``delta``
        Analyze Delta table structure, properties, and optimization.
        *Parameters:* ``table_path`` (required), ``analyze_history``
    """
    from pyspark_tools import server as _s

    if mode == "analyze":
        if not s3_path:
            return {"status": "error", "message": "s3_path is required for mode='analyze'"}
        return _s.analyze_s3_data_source(
            s3_path=s3_path,
            include_schema_inference=include_schema_inference,
        )

    elif mode == "delta":
        if not table_path:
            return {"status": "error", "message": "table_path is required for mode='delta'"}
        return _s.analyze_delta_table(
            table_path=table_path,
            analyze_history=analyze_history,
        )

    else:
        valid = "analyze, delta"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}


# ===========================================================================
# 14. ANALYTICS — Optimization effectiveness and usage analytics
# ===========================================================================
# Modes:
#   optimization - Analytics on optimization effectiveness and usage patterns
#   usage        - Usage statistics (conversions, patterns)
# ===========================================================================
@app.tool()
def analytics(
    mode: str,
    optimization_type: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Analytics on optimization effectiveness and usage patterns.

    **Modes:**

    ``optimization``
        Get analytics on optimization effectiveness.
        *Parameters:* ``optimization_type``, ``limit``

    ``usage``
        Get usage statistics including conversion history and pattern stats.
        *Parameters:* ``limit``
    """
    from pyspark_tools import server as _s

    if mode == "optimization":
        return _s.get_optimization_analytics(
            optimization_type=optimization_type,
            limit=limit,
        )

    elif mode == "usage":
        return _s.get_conversion_history(limit=limit)

    else:
        valid = "optimization, usage"
        return {"status": "error", "message": f"Unknown mode: {mode}. Valid: {valid}"}
