"""FastMCP server for SQL to PySpark conversion with code review and optimization."""

import json
from typing import Any, Dict, List, Optional, Union

from fastmcp import FastMCP

from .advanced_optimizer import AdvancedOptimizer
from .aws_glue_integration import (
    AWSGlueIntegration,
    DataCatalogTable,
    DataFormat,
    GlueJobConfig,
    GlueJobType,
)
from .batch_processor import BatchProcessor
from .code_reviewer import PySparkCodeReviewer
from .duplicate_detector import DuplicateDetector
from .memory_manager import MemoryManager
from .sql_converter import SQLToPySparkConverter

# Initialize components
memory = MemoryManager()
converter = SQLToPySparkConverter()
reviewer = PySparkCodeReviewer()
batch_processor = BatchProcessor(memory, converter)
duplicate_detector = DuplicateDetector()
advanced_optimizer = AdvancedOptimizer()
aws_glue_integration = AWSGlueIntegration()

# Create FastMCP app
app = FastMCP("PySpark Tools")


@app.tool()
def convert_sql_to_pyspark(
    sql_query: str,
    table_info: Optional[Dict] = None,
    dialect: Optional[str] = None,
    store_result: bool = True,
) -> Dict[str, Any]:
    """
    Convert SQL query to PySpark code with enhanced dialect support.

    Args:
        sql_query: The SQL query to convert
        table_info: Optional table metadata for better optimization
        dialect: Source SQL dialect (postgres, oracle, redshift, etc.)
        store_result: Whether to store the result in memory

    Returns:
        Dictionary containing PySpark code, optimizations, and conversion metadata
    """
    try:
        # Check if we have a cached conversion
        cached = memory.get_conversion(sql_query)
        if cached:
            return {
                "status": "success",
                "source": "cache",
                "pyspark_code": cached["pyspark_code"],
                "optimizations": (
                    cached["optimization_notes"].split("\n")
                    if cached["optimization_notes"]
                    else []
                ),
                "review_notes": cached["review_notes"],
                "dialect_used": cached.get("dialect", "unknown"),
                "complex_constructs": [],
                "warnings": [],
                "fallback_used": False,
            }

        # Convert SQL to PySpark with enhanced features
        result = converter.convert_sql_to_pyspark(sql_query, table_info, dialect)

        # Store result if requested
        if store_result:
            memory.store_conversion(
                sql_query=sql_query,
                pyspark_code=result.pyspark_code,
                optimization_notes="\n".join(result.optimizations),
                dialect=result.dialect_used,
            )

        return {
            "status": "success",
            "source": "converted",
            "pyspark_code": result.pyspark_code,
            "optimizations": result.optimizations,
            "warnings": result.warnings,
            "dialect_used": result.dialect_used,
            "complex_constructs": result.complex_constructs,
            "fallback_used": result.fallback_used,
            "sql_query": sql_query,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "sql_query": sql_query,
            "dialect": dialect,
        }


@app.tool()
def review_pyspark_code(
    code: str, focus_areas: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Review PySpark code for best practices, AWS Glue compatibility, and optimizations.

    Args:
        code: The PySpark code to review
        focus_areas: Optional list of areas to focus on: ["aws_glue", "performance", "best_practice", "style"]

    Returns:
        Dictionary containing review results and suggestions
    """
    try:
        # Review the code
        issues, metrics = reviewer.review_code(code)

        # Filter by focus areas if specified
        if focus_areas:
            issues = [issue for issue in issues if issue.category in focus_areas]

        # Generate report
        report = reviewer.generate_report(issues, metrics)

        # Store review in memory
        memory.store_context(
            f"review_{hash(code)}",
            {
                "code": code,
                "issues": [issue.__dict__ for issue in issues],
                "metrics": metrics,
                "report": report,
            },
        )

        return {
            "status": "success",
            "issues": [issue.__dict__ for issue in issues],
            "metrics": metrics,
            "report": report,
            "summary": {
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.severity == "error"]),
                "aws_glue_ready": len(
                    [
                        i
                        for i in issues
                        if i.category == "aws_glue" and i.severity == "error"
                    ]
                )
                == 0,
            },
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def optimize_pyspark_code(
    code: str, optimization_level: str = "standard"
) -> Dict[str, Any]:
    """
    Provide optimization suggestions for PySpark code.

    Args:
        code: The PySpark code to optimize
        optimization_level: Level of optimization ("basic", "standard", "aggressive")

    Returns:
        Dictionary containing optimization suggestions and improved code
    """
    try:
        # Review code first to identify issues
        issues, metrics = reviewer.review_code(code)

        # Generate optimization suggestions based on level
        optimizations = []

        if optimization_level in ["basic", "standard", "aggressive"]:
            # Performance optimizations
            perf_issues = [i for i in issues if i.category == "performance"]
            optimizations.extend([issue.suggestion for issue in perf_issues])

        if optimization_level in ["standard", "aggressive"]:
            # Best practice optimizations
            bp_issues = [i for i in issues if i.category == "best_practice"]
            optimizations.extend([issue.suggestion for issue in bp_issues])

        if optimization_level == "aggressive":
            # AWS Glue specific optimizations
            glue_issues = [i for i in issues if i.category == "aws_glue"]
            optimizations.extend([issue.suggestion for issue in glue_issues])

            # Additional aggressive optimizations
            optimizations.extend(
                [
                    "Consider using Delta Lake for ACID transactions",
                    "Implement data partitioning strategy",
                    "Use column-based file formats (Parquet)",
                    "Implement proper error handling and logging",
                    "Consider using Glue Data Catalog for metadata management",
                ]
            )

        # Remove duplicates
        optimizations = list(set(optimizations))

        return {
            "status": "success",
            "optimization_level": optimization_level,
            "suggestions": optimizations,
            "performance_score": max(0, 100 - (metrics["performance_issues"] * 10)),
            "aws_glue_compatibility": len(
                [
                    i
                    for i in issues
                    if i.category == "aws_glue" and i.severity == "error"
                ]
            )
            == 0,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_conversion_history(limit: int = 10) -> Dict[str, Any]:
    """
    Get recent SQL to PySpark conversion history.

    Args:
        limit: Maximum number of conversions to return

    Returns:
        Dictionary containing recent conversions
    """
    try:
        conversions = memory.get_recent_conversions(limit)

        return {
            "status": "success",
            "conversions": conversions,
            "count": len(conversions),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def search_conversions(query: str, limit: int = 5) -> Dict[str, Any]:
    """
    Search through stored SQL to PySpark conversions.

    Args:
        query: Search query to match against SQL or PySpark code
        limit: Maximum number of results to return

    Returns:
        Dictionary containing matching conversions
    """
    try:
        results = memory.search_conversions(query, limit)

        return {
            "status": "success",
            "results": results,
            "count": len(results),
            "query": query,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def store_context(key: str, value: Any) -> Dict[str, str]:
    """
    Store context information for future reference.

    Args:
        key: Context key
        value: Context value (can be any JSON-serializable type)

    Returns:
        Status message
    """
    try:
        memory.store_context(key, value)
        return {"status": "success", "message": f"Context stored with key: {key}"}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_context(key: str) -> Dict[str, Any]:
    """
    Retrieve stored context information.

    Args:
        key: Context key to retrieve

    Returns:
        Dictionary containing the context value
    """
    try:
        value = memory.get_context(key)

        if value is not None:
            return {"status": "success", "key": key, "value": value}
        else:
            return {
                "status": "not_found",
                "key": key,
                "message": "Context key not found",
            }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_aws_glue_job_template(
    job_name: str,
    source_format: str = "parquet",
    target_format: str = "parquet",
    include_bookmarking: bool = True,
    use_dynamic_frame: bool = True,
    source_database: Optional[str] = None,
    source_table: Optional[str] = None,
    target_database: Optional[str] = None,
    target_table: Optional[str] = None,
    transformation_sql: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate an enhanced AWS Glue job template with DynamicFrame support.

    Args:
        job_name: Name of the Glue job
        source_format: Source data format (parquet, csv, json, etc.)
        target_format: Target data format
        include_bookmarking: Whether to include job bookmarking
        use_dynamic_frame: Whether to use DynamicFrame instead of DataFrame
        source_database: Source database name for Data Catalog
        source_table: Source table name for Data Catalog
        target_database: Target database name for Data Catalog
        target_table: Target table name for Data Catalog
        transformation_sql: Optional SQL transformation logic

    Returns:
        Dictionary containing the enhanced Glue job template
    """
    try:
        # Create configuration
        config = GlueJobConfig(
            job_name=job_name,
            source_format=DataFormat(source_format.lower()),
            target_format=DataFormat(target_format.lower()),
            include_bookmarking=include_bookmarking,
            use_dynamic_frame=use_dynamic_frame,
        )

        # Create table configurations if provided
        source_table_config = None
        if source_database and source_table:
            source_table_config = DataCatalogTable(
                database_name=source_database,
                table_name=source_table,
                format=DataFormat(source_format.lower()),
            )

        target_table_config = None
        if target_database and target_table:
            target_table_config = DataCatalogTable(
                database_name=target_database,
                table_name=target_table,
                format=DataFormat(target_format.lower()),
            )

        # Generate enhanced template
        result = aws_glue_integration.generate_enhanced_glue_job_template(
            config=config,
            source_table=source_table_config,
            target_table=target_table_config,
            transformation_sql=transformation_sql,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def batch_process_files(
    file_paths: List[str],
    output_dir: Optional[str] = None,
    job_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process multiple SQL files in batch.

    Args:
        file_paths: List of file paths to process
        output_dir: Optional output directory for converted files
        job_name: Optional name for the batch job

    Returns:
        Dictionary containing batch processing results
    """
    try:
        result = batch_processor.process_files(
            file_paths=file_paths, output_dir=output_dir, job_name=job_name
        )

        return {
            "status": "success",
            "job_id": result.job_id,
            "job_name": result.job_name,
            "batch_status": result.status.value,
            "statistics": {
                "total_files": result.total_files,
                "successful_files": result.successful_files,
                "failed_files": result.failed_files,
                "total_queries": result.total_queries,
                "successful_queries": result.successful_queries,
                "failed_queries": result.failed_queries,
                "processing_time": result.processing_time,
            },
            "output_directory": result.output_directory,
            "optimization_summary": result.optimization_summary,
            "error_count": len(result.error_summary) if result.error_summary else 0,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def batch_process_directory(
    directory_path: str,
    output_dir: Optional[str] = None,
    job_name: Optional[str] = None,
    recursive: bool = True,
) -> Dict[str, Any]:
    """
    Process all SQL files in a directory.

    Args:
        directory_path: Path to directory containing SQL files
        output_dir: Optional output directory for converted files
        job_name: Optional name for the batch job
        recursive: Whether to search subdirectories

    Returns:
        Dictionary containing batch processing results
    """
    try:
        result = batch_processor.process_directory(
            directory_path=directory_path,
            output_dir=output_dir,
            job_name=job_name,
            recursive=recursive,
        )

        return {
            "status": "success",
            "job_id": result.job_id,
            "job_name": result.job_name,
            "batch_status": result.status.value,
            "statistics": {
                "total_files": result.total_files,
                "successful_files": result.successful_files,
                "failed_files": result.failed_files,
                "total_queries": result.total_queries,
                "successful_queries": result.successful_queries,
                "failed_queries": result.failed_queries,
                "processing_time": result.processing_time,
            },
            "output_directory": result.output_directory,
            "optimization_summary": result.optimization_summary,
            "error_count": len(result.error_summary) if result.error_summary else 0,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def extract_sql_from_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    Extract SQL queries from a PDF document.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Dictionary containing extracted SQL queries
    """
    try:
        from .file_utils import extract_sql_from_pdf as extract_pdf

        extracted_queries = extract_pdf(pdf_path)

        return {
            "status": "success",
            "pdf_path": pdf_path,
            "query_count": len(extracted_queries),
            "queries": [
                {
                    "query": q.query,
                    "page_number": q.page_number,
                    "line_number": q.line_number,
                    "confidence": q.confidence,
                }
                for q in extracted_queries
            ],
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "pdf_path": pdf_path}


@app.tool()
def get_batch_status(job_id: int) -> Dict[str, Any]:
    """
    Get the current status of a batch processing job.

    Args:
        job_id: ID of the batch job

    Returns:
        Dictionary containing job status and progress
    """
    try:
        # Check active jobs first
        progress = batch_processor.get_batch_status(job_id)

        if progress:
            return {
                "status": "success",
                "job_id": job_id,
                "batch_status": progress.status.value,
                "progress": {
                    "total_files": progress.total_files,
                    "processed_files": progress.processed_files,
                    "successful_files": progress.successful_files,
                    "failed_files": progress.failed_files,
                    "total_queries": progress.total_queries,
                    "successful_queries": progress.successful_queries,
                    "failed_queries": progress.failed_queries,
                    "current_file": progress.current_file,
                    "estimated_completion": (
                        progress.estimated_completion.isoformat()
                        if progress.estimated_completion
                        else None
                    ),
                },
                "start_time": progress.start_time.isoformat(),
            }

        # Check database for completed jobs
        batch_job = memory.get_batch_job(job_id)
        if batch_job:
            return {
                "status": "success",
                "job_id": job_id,
                "batch_status": batch_job.status,
                "job_name": batch_job.job_name,
                "output_directory": batch_job.output_directory,
                "created_at": batch_job.created_at,
            }

        return {
            "status": "not_found",
            "job_id": job_id,
            "message": "Batch job not found",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def cancel_batch_job(job_id: int) -> Dict[str, Any]:
    """
    Cancel a running batch processing job.

    Args:
        job_id: ID of the batch job to cancel

    Returns:
        Dictionary containing cancellation result
    """
    try:
        success = batch_processor.cancel_batch_job(job_id)

        return {
            "status": "success" if success else "failed",
            "job_id": job_id,
            "cancelled": success,
            "message": (
                "Job cancelled successfully"
                if success
                else "Job not found or not running"
            ),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_active_batch_jobs() -> Dict[str, Any]:
    """
    Get list of all currently active batch jobs.

    Returns:
        Dictionary containing list of active jobs
    """
    try:
        active_jobs = batch_processor.get_active_jobs()

        return {
            "status": "success",
            "active_job_count": len(active_jobs),
            "jobs": [
                {
                    "job_id": job.job_id,
                    "status": job.status.value,
                    "total_files": job.total_files,
                    "processed_files": job.processed_files,
                    "successful_files": job.successful_files,
                    "failed_files": job.failed_files,
                    "current_file": job.current_file,
                    "start_time": job.start_time.isoformat(),
                    "estimated_completion": (
                        job.estimated_completion.isoformat()
                        if job.estimated_completion
                        else None
                    ),
                }
                for job in active_jobs
            ],
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_recent_batch_jobs(limit: int = 10) -> Dict[str, Any]:
    """
    Get recent batch jobs from the database.

    Args:
        limit: Maximum number of jobs to return

    Returns:
        Dictionary containing recent batch jobs
    """
    try:
        recent_jobs = memory.get_recent_batch_jobs(limit)

        return {
            "status": "success",
            "job_count": len(recent_jobs),
            "jobs": [
                {
                    "job_id": job.id,
                    "job_name": job.job_name,
                    "status": job.status,
                    "output_directory": job.output_directory,
                    "created_at": job.created_at,
                    "file_count": (
                        len(json.loads(job.input_files)) if job.input_files else 0
                    ),
                }
                for job in recent_jobs
            ],
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def analyze_code_patterns(
    code_samples: List[str], similarity_threshold: float = 0.8
) -> Dict[str, Any]:
    """
    Analyze code samples to identify duplicate patterns and refactoring opportunities.

    Args:
        code_samples: List of PySpark code samples to analyze
        similarity_threshold: Threshold for pattern similarity (0.0 to 1.0)

    Returns:
        Dictionary containing pattern analysis results
    """
    try:
        # Set similarity threshold
        duplicate_detector.similarity_threshold = similarity_threshold

        # Analyze patterns
        analysis = duplicate_detector.analyze_patterns(code_samples)

        # Store patterns in memory for future reference
        for pattern in analysis.patterns:
            memory.store_duplicate_pattern(
                pattern_hash=pattern.pattern_hash,
                pattern_description=pattern.description,
                code_template=pattern.code_template,
            )

        return {
            "status": "success",
            "analysis": {
                "patterns_detected": len(analysis.patterns),
                "total_matches": len(analysis.matches),
                "duplicate_groups": len(analysis.duplicate_groups),
                "refactoring_opportunities": len(analysis.refactoring_opportunities),
            },
            "patterns": [
                {
                    "pattern_id": p.pattern_id,
                    "description": p.description,
                    "usage_count": p.usage_count,
                    "parameters": p.parameters,
                    "similarity_threshold": p.similarity_threshold,
                }
                for p in analysis.patterns
            ],
            "refactoring_opportunities": analysis.refactoring_opportunities,
            "statistics": analysis.statistics,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_utility_functions(
    code_samples: List[str], min_usage_count: int = 2
) -> Dict[str, Any]:
    """
    Generate utility functions from detected code patterns.

    Args:
        code_samples: List of PySpark code samples to analyze
        min_usage_count: Minimum usage count for pattern to generate utility function

    Returns:
        Dictionary containing generated utility functions
    """
    try:
        # Analyze patterns first
        analysis = duplicate_detector.analyze_patterns(code_samples)

        # Filter patterns by usage count
        qualifying_patterns = [
            p for p in analysis.patterns if p.usage_count >= min_usage_count
        ]

        # Generate utility functions
        utility_functions = duplicate_detector.extract_common_functions(
            qualifying_patterns
        )

        # Generate complete utilities module
        utilities_module = duplicate_detector.generate_utilities_module(
            utility_functions
        )

        return {
            "status": "success",
            "utility_functions_count": len(utility_functions),
            "functions": [
                {
                    "function_name": func.function_name,
                    "description": func.description,
                    "parameters": func.parameters,
                    "pattern_id": func.pattern_id,
                    "function_code": func.function_code,
                }
                for func in utility_functions
            ],
            "utilities_module": utilities_module,
            "patterns_used": len(qualifying_patterns),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def refactor_code_with_patterns(
    original_code: str, code_samples: List[str]
) -> Dict[str, Any]:
    """
    Refactor code by replacing duplicate patterns with utility function calls.

    Args:
        original_code: The code to refactor
        code_samples: Code samples to analyze for patterns (including original_code)

    Returns:
        Dictionary containing refactored code and refactoring report
    """
    try:
        # Include original code in analysis
        all_samples = code_samples + [original_code]

        # Analyze patterns
        analysis = duplicate_detector.analyze_patterns(all_samples)

        # Generate utility functions
        utility_functions = duplicate_detector.extract_common_functions(
            analysis.patterns
        )

        # Refactor the original code
        refactored_code = duplicate_detector.refactor_code_with_utilities(
            original_code, utility_functions
        )

        # Generate utilities module
        utilities_module = duplicate_detector.generate_utilities_module(
            utility_functions
        )

        return {
            "status": "success",
            "original_code": original_code,
            "refactored_code": refactored_code,
            "utilities_module": utilities_module,
            "refactoring_report": {
                "patterns_found": len(analysis.patterns),
                "utility_functions_generated": len(utility_functions),
                "estimated_lines_saved": sum(
                    opp.get("estimated_lines_saved", 0)
                    for opp in analysis.refactoring_opportunities
                ),
                "complexity_reduction": (
                    "high" if len(utility_functions) >= 3 else "medium"
                ),
            },
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_stored_patterns(min_usage_count: int = 1, limit: int = 20) -> Dict[str, Any]:
    """
    Get stored duplicate patterns from the database.

    Args:
        min_usage_count: Minimum usage count to include
        limit: Maximum number of patterns to return

    Returns:
        Dictionary containing stored patterns
    """
    try:
        patterns = memory.get_common_patterns(min_usage_count, limit)

        return {
            "status": "success",
            "pattern_count": len(patterns),
            "patterns": [
                {
                    "pattern_id": p.id,
                    "pattern_hash": p.pattern_hash,
                    "description": p.pattern_description,
                    "usage_count": p.usage_count,
                    "created_at": p.created_at,
                    "code_template": p.code_template,
                }
                for p in patterns
            ],
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def search_code_patterns(query: str, limit: int = 10) -> Dict[str, Any]:
    """
    Search stored code patterns by description or template.

    Args:
        query: Search query to match against pattern descriptions or templates
        limit: Maximum number of results to return

    Returns:
        Dictionary containing matching patterns
    """
    try:
        patterns = memory.search_patterns(query, limit)

        return {
            "status": "success",
            "query": query,
            "result_count": len(patterns),
            "patterns": [
                {
                    "pattern_id": p.id,
                    "pattern_hash": p.pattern_hash,
                    "description": p.pattern_description,
                    "usage_count": p.usage_count,
                    "created_at": p.created_at,
                    "code_template": p.code_template,
                }
                for p in patterns
            ],
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def get_pattern_statistics() -> Dict[str, Any]:
    """
    Get statistics about detected and stored patterns.

    Returns:
        Dictionary containing pattern statistics
    """
    try:
        # Get statistics from duplicate detector
        detector_stats = duplicate_detector.get_pattern_statistics()

        # Get statistics from database
        all_patterns = memory.get_common_patterns(min_usage=1, limit=1000)

        if all_patterns:
            db_stats = {
                "stored_patterns": len(all_patterns),
                "total_stored_usage": sum(p.usage_count for p in all_patterns),
                "average_stored_usage": sum(p.usage_count for p in all_patterns)
                / len(all_patterns),
                "high_usage_stored_patterns": len(
                    [p for p in all_patterns if p.usage_count >= 5]
                ),
            }
        else:
            db_stats = {
                "stored_patterns": 0,
                "total_stored_usage": 0,
                "average_stored_usage": 0,
                "high_usage_stored_patterns": 0,
            }

        return {
            "status": "success",
            "detector_statistics": detector_stats,
            "database_statistics": db_stats,
            "combined_statistics": {
                "total_patterns_analyzed": detector_stats.get("total_patterns", 0),
                "total_patterns_stored": db_stats["stored_patterns"],
                "refactoring_potential": detector_stats.get(
                    "refactoring_candidates", 0
                ),
            },
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def analyze_data_flow(
    pyspark_code: str, table_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Analyze data flow patterns in PySpark code.

    Args:
        pyspark_code: PySpark code to analyze
        table_info: Optional table metadata for better analysis

    Returns:
        Dictionary containing data flow analysis results
    """
    try:
        analysis = advanced_optimizer.analyze_data_flow(pyspark_code, table_info)

        return {
            "status": "success",
            "analysis": {
                "total_nodes": len(analysis.nodes),
                "execution_order": analysis.execution_order,
                "join_operations": len(analysis.join_operations),
                "aggregation_operations": len(analysis.aggregation_operations),
                "filter_operations": len(analysis.filter_operations),
                "estimated_total_cost": analysis.estimated_total_cost,
                "nodes": {
                    node_id: {
                        "operation": node.operation,
                        "table_name": node.table_name,
                        "estimated_size_mb": node.estimated_size_mb,
                        "estimated_rows": node.estimated_rows,
                        "dependencies": node.dependencies,
                    }
                    for node_id, node in analysis.nodes.items()
                },
                "join_details": analysis.join_operations,
                "aggregation_details": analysis.aggregation_operations,
                "filter_details": analysis.filter_operations,
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "pyspark_code": (
                pyspark_code[:200] + "..." if len(pyspark_code) > 200 else pyspark_code
            ),
        }


@app.tool()
def suggest_partitioning_strategy(
    pyspark_code: str, table_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Suggest optimal partitioning strategies based on query patterns.

    Args:
        pyspark_code: PySpark code to analyze
        table_info: Optional table metadata

    Returns:
        Dictionary containing partitioning strategy recommendations
    """
    try:
        analysis = advanced_optimizer.analyze_data_flow(pyspark_code, table_info)
        strategies = advanced_optimizer.suggest_partitioning_strategy(
            analysis, table_info
        )

        return {
            "status": "success",
            "strategies": [
                {
                    "table_name": strategy.table_name,
                    "partition_columns": strategy.partition_columns,
                    "partition_type": strategy.partition_type,
                    "estimated_partitions": strategy.estimated_partitions,
                    "reasoning": strategy.reasoning,
                    "performance_impact": strategy.performance_impact,
                }
                for strategy in strategies
            ],
            "total_strategies": len(strategies),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "pyspark_code": (
                pyspark_code[:200] + "..." if len(pyspark_code) > 200 else pyspark_code
            ),
        }


@app.tool()
def recommend_join_strategy(
    pyspark_code: str, table_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Recommend specific join strategies based on estimated table sizes.

    Args:
        pyspark_code: PySpark code to analyze
        table_info: Optional table metadata

    Returns:
        Dictionary containing join optimization recommendations
    """
    try:
        analysis = advanced_optimizer.analyze_data_flow(pyspark_code, table_info)
        optimizations = advanced_optimizer.recommend_join_strategy(analysis, table_info)

        return {
            "status": "success",
            "optimizations": [
                {
                    "join_id": opt.join_id,
                    "left_table": opt.left_table,
                    "right_table": opt.right_table,
                    "join_type": opt.join_type,
                    "recommended_strategy": opt.recommended_strategy,
                    "estimated_left_size": opt.estimated_left_size,
                    "estimated_right_size": opt.estimated_right_size,
                    "reasoning": opt.reasoning,
                    "performance_impact": opt.performance_impact,
                }
                for opt in optimizations
            ],
            "total_optimizations": len(optimizations),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "pyspark_code": (
                pyspark_code[:200] + "..." if len(pyspark_code) > 200 else pyspark_code
            ),
        }


@app.tool()
def estimate_performance_impact(
    pyspark_code: str, table_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Estimate performance impact for optimization suggestions.

    Args:
        pyspark_code: PySpark code to analyze
        table_info: Optional table metadata

    Returns:
        Dictionary containing performance impact estimates
    """
    try:
        recommendations = advanced_optimizer.generate_comprehensive_recommendations(
            pyspark_code, table_info
        )
        estimates = advanced_optimizer.estimate_performance_impact(recommendations)

        return {
            "status": "success",
            "estimates": {
                opt_id: {
                    "optimization_type": estimate.optimization_type.value,
                    "estimated_improvement": estimate.estimated_improvement,
                    "confidence_level": estimate.confidence_level,
                    "resource_impact": estimate.resource_impact,
                    "implementation_complexity": estimate.implementation_complexity,
                    "prerequisites": estimate.prerequisites,
                }
                for opt_id, estimate in estimates.items()
            },
            "total_estimates": len(estimates),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "pyspark_code": (
                pyspark_code[:200] + "..." if len(pyspark_code) > 200 else pyspark_code
            ),
        }


@app.tool()
def generate_comprehensive_optimizations(
    pyspark_code: str, table_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Generate comprehensive optimization recommendations for PySpark code.

    Args:
        pyspark_code: PySpark code to optimize
        table_info: Optional table metadata

    Returns:
        Dictionary containing comprehensive optimization recommendations
    """
    try:
        recommendations = advanced_optimizer.generate_comprehensive_recommendations(
            pyspark_code, table_info
        )

        # Store performance metrics for tracking
        for rec in recommendations:
            memory.store_performance_metric(
                conversion_id=None,
                metric_type="optimization_recommendation",
                metric_value=rec.performance_estimate.estimated_improvement,
                optimization_applied=rec.optimization_type.value,
            )

        return {
            "status": "success",
            "recommendations": [
                {
                    "optimization_id": rec.optimization_id,
                    "optimization_type": rec.optimization_type.value,
                    "title": rec.title,
                    "description": rec.description,
                    "code_changes": rec.code_changes,
                    "priority": rec.priority,
                    "applicable_conditions": rec.applicable_conditions,
                    "performance_estimate": {
                        "estimated_improvement": rec.performance_estimate.estimated_improvement,
                        "confidence_level": rec.performance_estimate.confidence_level,
                        "resource_impact": rec.performance_estimate.resource_impact,
                        "implementation_complexity": rec.performance_estimate.implementation_complexity,
                        "prerequisites": rec.performance_estimate.prerequisites,
                    },
                }
                for rec in recommendations
            ],
            "total_recommendations": len(recommendations),
            "high_priority_count": len(
                [r for r in recommendations if r.priority == "high"]
            ),
            "estimated_total_improvement": sum(
                r.performance_estimate.estimated_improvement for r in recommendations
            ),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "pyspark_code": (
                pyspark_code[:200] + "..." if len(pyspark_code) > 200 else pyspark_code
            ),
        }


@app.tool()
def get_optimization_analytics(
    optimization_type: Optional[str] = None, limit: int = 50
) -> Dict[str, Any]:
    """
    Get analytics on optimization effectiveness and usage patterns.

    Args:
        optimization_type: Optional filter by optimization type
        limit: Maximum number of metrics to return

    Returns:
        Dictionary containing optimization analytics
    """
    try:
        if optimization_type:
            metrics = memory.get_metrics_by_type(optimization_type, limit)
            effectiveness = memory.get_optimization_effectiveness(optimization_type)
        else:
            metrics = memory.get_recent_metrics(limit)
            effectiveness = {}

        return {
            "status": "success",
            "metrics": [
                {
                    "metric_id": metric.id,
                    "conversion_id": metric.conversion_id,
                    "metric_type": metric.metric_type,
                    "metric_value": metric.metric_value,
                    "optimization_applied": metric.optimization_applied,
                    "created_at": metric.created_at,
                }
                for metric in metrics
            ],
            "effectiveness": effectiveness,
            "total_metrics": len(metrics),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "optimization_type": optimization_type,
        }


@app.tool()
def convert_dataframe_to_dynamic_frame(
    pyspark_code: str,
    source_database: str,
    source_table: str,
    target_database: str,
    target_table: str,
) -> Dict[str, Any]:
    """
    Convert existing PySpark DataFrame code to use DynamicFrames.

    Args:
        pyspark_code: Existing PySpark DataFrame code
        source_database: Source database name
        source_table: Source table name
        target_database: Target database name
        target_table: Target table name

    Returns:
        Dictionary containing DynamicFrame-based code
    """
    try:
        source_table_config = DataCatalogTable(
            database_name=source_database,
            table_name=source_table,
        )

        target_table_config = DataCatalogTable(
            database_name=target_database,
            table_name=target_table,
        )

        result = aws_glue_integration.generate_dynamic_frame_conversion_template(
            pyspark_code=pyspark_code,
            source_table=source_table_config,
            target_table=target_table_config,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_glue_job_with_sql_conversion(
    sql_query: str,
    job_name: str,
    source_database: str,
    source_table: str,
    target_database: str,
    target_table: str,
    source_format: str = "parquet",
    target_format: str = "parquet",
    include_bookmarking: bool = True,
) -> Dict[str, Any]:
    """
    Generate a Glue job template that includes SQL to PySpark conversion with DynamicFrame support.

    Args:
        sql_query: SQL query to convert and include
        job_name: Name of the Glue job
        source_database: Source database name
        source_table: Source table name
        target_database: Target database name
        target_table: Target table name
        source_format: Source data format
        target_format: Target data format
        include_bookmarking: Whether to include job bookmarking

    Returns:
        Dictionary containing the complete Glue job with SQL conversion
    """
    try:
        config = GlueJobConfig(
            job_name=job_name,
            source_format=DataFormat(source_format.lower()),
            target_format=DataFormat(target_format.lower()),
            include_bookmarking=include_bookmarking,
            use_dynamic_frame=True,
        )

        source_table_config = DataCatalogTable(
            database_name=source_database,
            table_name=source_table,
            format=DataFormat(source_format.lower()),
        )

        target_table_config = DataCatalogTable(
            database_name=target_database,
            table_name=target_table,
            format=DataFormat(target_format.lower()),
        )

        result = aws_glue_integration.generate_glue_job_with_sql_conversion(
            sql_query=sql_query,
            config=config,
            source_table=source_table_config,
            target_table=target_table_config,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_data_catalog_table_definition(
    database_name: str,
    table_name: str,
    s3_location: str,
    data_format: str = "parquet",
    columns: Optional[List[Dict[str, str]]] = None,
    partition_keys: Optional[List[str]] = None,
    enable_schema_evolution: bool = True,
) -> Dict[str, Any]:
    """
    Generate AWS Glue Data Catalog table definition.

    Args:
        database_name: Name of the Glue database
        table_name: Name of the table
        s3_location: S3 location of the data
        data_format: Data format (parquet, csv, json, etc.)
        columns: List of column definitions with name, type, and optional comment
        partition_keys: List of partition key names
        enable_schema_evolution: Whether to enable schema evolution

    Returns:
        Dictionary containing table definition and creation commands
    """
    try:
        from .aws_glue_integration import (
            ColumnInfo,
            PartitionInfo,
            SchemaEvolutionConfig,
        )

        # Convert columns to ColumnInfo objects
        column_objects = []
        if columns:
            for col in columns:
                column_objects.append(
                    ColumnInfo(
                        name=col["name"],
                        type=col["type"],
                        comment=col.get("comment", ""),
                    )
                )

        # Convert partition keys to PartitionInfo objects
        partition_objects = []
        if partition_keys:
            for key in partition_keys:
                partition_objects.append(
                    PartitionInfo(
                        key=key,
                        type="string",  # Default type, could be inferred
                        comment=f"Partition key: {key}",
                    )
                )

        # Create table configuration
        table_config = DataCatalogTable(
            database_name=database_name,
            table_name=table_name,
            s3_location=s3_location,
            format=DataFormat(data_format.lower()),
            columns=column_objects,
            partitions=partition_objects,
        )

        # Create schema evolution config
        schema_config = SchemaEvolutionConfig(
            enable_schema_evolution=enable_schema_evolution,
        )

        result = aws_glue_integration.generate_data_catalog_table_definition(
            table=table_config,
            schema_evolution_config=schema_config,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def detect_schema_from_sample_data(
    sample_data: Union[Dict[str, Any], List[Dict[str, Any]]],
    table_name: str,
    infer_partitions: bool = True,
) -> Dict[str, Any]:
    """
    Detect schema from sample data and generate table definition.

    Args:
        sample_data: Sample data as dictionary or list of dictionaries
        table_name: Name for the table
        infer_partitions: Whether to infer partition keys from data patterns

    Returns:
        Dictionary containing detected schema and recommendations
    """
    try:
        result = aws_glue_integration.detect_schema_from_sample_data(
            sample_data=sample_data,
            table_name=table_name,
            infer_partitions=infer_partitions,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_schema_evolution_strategy(
    current_columns: List[Dict[str, str]],
    new_columns: List[Dict[str, str]],
    merge_behavior: str = "merge",
    case_sensitive: bool = False,
) -> Dict[str, Any]:
    """
    Generate schema evolution strategy for handling schema changes.

    Args:
        current_columns: Current table schema as list of column definitions
        new_columns: New/target schema as list of column definitions
        merge_behavior: Evolution behavior (merge, overwrite, fail)
        case_sensitive: Whether column name comparison is case sensitive

    Returns:
        Dictionary containing evolution strategy and migration steps
    """
    try:
        from .aws_glue_integration import ColumnInfo, SchemaEvolutionConfig

        # Convert to ColumnInfo objects
        current_schema = [
            ColumnInfo(
                name=col["name"], type=col["type"], comment=col.get("comment", "")
            )
            for col in current_columns
        ]

        new_schema = [
            ColumnInfo(
                name=col["name"], type=col["type"], comment=col.get("comment", "")
            )
            for col in new_columns
        ]

        # Create evolution config
        evolution_config = SchemaEvolutionConfig(
            enable_schema_evolution=True,
            merge_behavior=merge_behavior,
            case_sensitive=case_sensitive,
        )

        result = aws_glue_integration.generate_schema_evolution_strategy(
            current_schema=current_schema,
            new_schema=new_schema,
            evolution_config=evolution_config,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def analyze_s3_optimization_opportunities(
    s3_location: str,
    database_name: str,
    table_name: str,
    data_format: str = "parquet",
    query_patterns: Optional[List[str]] = None,
    data_size_gb: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Analyze S3 data layout and suggest optimization opportunities.

    Args:
        s3_location: S3 location of the data
        database_name: Database name
        table_name: Table name
        data_format: Data format (parquet, csv, json, etc.)
        query_patterns: Common query patterns for optimization
        data_size_gb: Estimated data size in GB

    Returns:
        Dictionary containing optimization analysis and recommendations
    """
    try:
        # Create table info
        table_info = DataCatalogTable(
            database_name=database_name,
            table_name=table_name,
            s3_location=s3_location,
            format=DataFormat(data_format.lower()),
        )

        result = aws_glue_integration.analyze_s3_optimization_opportunities(
            s3_location=s3_location,
            table_info=table_info,
            query_patterns=query_patterns,
            data_size_gb=data_size_gb,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_s3_optimization_strategy(
    database_name: str,
    table_name: str,
    s3_location: str,
    data_format: str = "parquet",
    target_file_size_mb: int = 128,
    compression_type: str = "snappy",
    enable_small_file_optimization: bool = True,
    query_patterns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Generate comprehensive S3 optimization strategy.

    Args:
        database_name: Database name
        table_name: Table name
        s3_location: S3 location of the data
        data_format: Data format
        target_file_size_mb: Target file size in MB
        compression_type: Compression type (snappy, gzip, lz4, zstd)
        enable_small_file_optimization: Whether to enable small file optimization
        query_patterns: Common query patterns

    Returns:
        Dictionary containing optimization strategy and implementation
    """
    try:
        from .aws_glue_integration import S3OptimizationConfig

        # Create table info
        table_info = DataCatalogTable(
            database_name=database_name,
            table_name=table_name,
            s3_location=s3_location,
            format=DataFormat(data_format.lower()),
        )

        # Create optimization config
        optimization_config = S3OptimizationConfig(
            target_file_size_mb=target_file_size_mb,
            compression_type=compression_type,
            enable_small_file_optimization=enable_small_file_optimization,
        )

        result = aws_glue_integration.generate_s3_optimization_strategy(
            table_info=table_info,
            optimization_config=optimization_config,
            query_patterns=query_patterns,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_small_files_consolidation_job(
    source_database: str,
    source_table: str,
    target_database: str,
    target_table: str,
    target_file_size_mb: int = 128,
    consolidation_strategy: str = "coalesce",
) -> Dict[str, Any]:
    """
    Generate Glue job for small files consolidation.

    Args:
        source_database: Source database name
        source_table: Source table name
        target_database: Target database name
        target_table: Target table name
        target_file_size_mb: Target file size in MB
        consolidation_strategy: Strategy for consolidation (coalesce, repartition)

    Returns:
        Dictionary containing consolidation job template
    """
    try:
        # Create table configurations
        source_table_config = DataCatalogTable(
            database_name=source_database,
            table_name=source_table,
        )

        target_table_config = DataCatalogTable(
            database_name=target_database,
            table_name=target_table,
        )

        result = aws_glue_integration.generate_small_files_consolidation_job(
            source_table=source_table_config,
            target_table=target_table_config,
            target_file_size_mb=target_file_size_mb,
            consolidation_strategy=consolidation_strategy,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_incremental_processing_job(
    source_database: str,
    source_table: str,
    target_database: str,
    target_table: str,
    incremental_column: str,
    incremental_strategy: str = "timestamp",
    transformation_sql: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate Glue job with incremental processing and job bookmarking.

    Args:
        source_database: Source database name
        source_table: Source table name
        target_database: Target database name
        target_table: Target table name
        incremental_column: Column used for incremental processing
        incremental_strategy: Strategy for incremental processing (timestamp, watermark, bookmark)
        transformation_sql: Optional SQL transformation logic

    Returns:
        Dictionary containing incremental processing job template
    """
    try:
        # Create table configurations
        source_table_config = DataCatalogTable(
            database_name=source_database,
            table_name=source_table,
        )

        target_table_config = DataCatalogTable(
            database_name=target_database,
            table_name=target_table,
        )

        result = aws_glue_integration.generate_incremental_processing_job(
            source_table=source_table_config,
            target_table=target_table_config,
            incremental_column=incremental_column,
            incremental_strategy=incremental_strategy,
            transformation_sql=transformation_sql,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_job_bookmark_configuration(
    job_name: str,
    bookmark_strategy: str = "enable",
    transformation_context_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Generate job bookmark configuration for Glue jobs.

    Args:
        job_name: Name of the Glue job
        bookmark_strategy: Bookmark strategy (enable, disable, pause)
        transformation_context_keys: List of transformation context keys

    Returns:
        Dictionary containing bookmark configuration
    """
    try:
        result = aws_glue_integration.generate_job_bookmark_configuration(
            job_name=job_name,
            bookmark_strategy=bookmark_strategy,
            transformation_context_keys=transformation_context_keys,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_change_data_capture_job(
    source_database: str,
    source_table: str,
    target_database: str,
    target_table: str,
    cdc_column: str = "last_modified",
    cdc_strategy: str = "upsert",
    primary_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Generate Glue job for Change Data Capture (CDC) processing.

    Args:
        source_database: Source database name
        source_table: Source table name
        target_database: Target database name
        target_table: Target table name
        cdc_column: Column indicating change timestamp
        cdc_strategy: CDC processing strategy (upsert, append, merge)
        primary_keys: Primary key columns for upsert operations

    Returns:
        Dictionary containing CDC job template
    """
    try:
        # Create table configurations
        source_table_config = DataCatalogTable(
            database_name=source_database,
            table_name=source_table,
        )

        target_table_config = DataCatalogTable(
            database_name=target_database,
            table_name=target_table,
        )

        result = aws_glue_integration.generate_change_data_capture_job(
            source_table=source_table_config,
            target_table=target_table_config,
            cdc_column=cdc_column,
            cdc_strategy=cdc_strategy,
            primary_keys=primary_keys,
        )

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def generate_glue_job_properties(
    job_name: str,
    job_type: str = "etl",
    worker_type: str = "G.1X",
    number_of_workers: int = 2,
    max_retries: int = 0,
    timeout: int = 2880,
    glue_version: str = "4.0",
    enable_continuous_logging: bool = True,
    enable_metrics: bool = True,
    enable_spark_ui: bool = True,
) -> Dict[str, Any]:
    """
    Generate AWS Glue job properties for job creation via AWS CLI or SDK.

    Args:
        job_name: Name of the Glue job
        job_type: Type of job (etl, streaming, pythonshell)
        worker_type: Worker type (G.1X, G.2X, etc.)
        number_of_workers: Number of workers
        max_retries: Maximum number of retries
        timeout: Job timeout in minutes
        glue_version: Glue version to use
        enable_continuous_logging: Enable continuous CloudWatch logging
        enable_metrics: Enable CloudWatch metrics
        enable_spark_ui: Enable Spark UI

    Returns:
        Dictionary containing Glue job properties
    """
    try:
        config = GlueJobConfig(
            job_name=job_name,
            job_type=GlueJobType(job_type.lower()),
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            max_retries=max_retries,
            timeout=timeout,
            glue_version=glue_version,
            enable_continuous_logging=enable_continuous_logging,
            enable_metrics=enable_metrics,
            enable_spark_ui=enable_spark_ui,
        )

        properties = aws_glue_integration._get_glue_job_properties(config)

        return {
            "status": "success",
            "job_properties": properties,
            "aws_cli_command": f"""aws glue create-job --cli-input-json '{json.dumps(properties, indent=2)}'""",
            "terraform_resource": f"""resource "aws_glue_job" "{job_name.replace('-', '_')}" {{
  name         = "{job_name}"
  role_arn     = "{properties['Role']}"
  glue_version = "{properties['GlueVersion']}"
  
  command {{
    name            = "{properties['Command']['Name']}"
    script_location = "{properties['Command']['ScriptLocation']}"
    python_version  = "{properties['Command']['PythonVersion']}"
  }}
  
  default_arguments = {{
{chr(10).join([f'    "{k}" = "{v}"' for k, v in properties['DefaultArguments'].items()])}
  }}
  
  worker_type       = "{properties['WorkerType']}"
  number_of_workers = {properties['NumberOfWorkers']}
  max_retries      = {properties['MaxRetries']}
  timeout          = {properties['Timeout']}
}}""",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # For development/testing
    app.run(debug=True)
