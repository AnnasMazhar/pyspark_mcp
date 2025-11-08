"""FastMCP server for SQL to PySpark conversion with code review and optimization."""

import json
from typing import Dict, List, Optional, Any
from fastmcp import FastMCP
from .memory_manager import MemoryManager
from .sql_converter import SQLToPySparkConverter
from .code_reviewer import PySparkCodeReviewer


# Initialize components
memory = MemoryManager()
converter = SQLToPySparkConverter()
reviewer = PySparkCodeReviewer()

# Create FastMCP app
app = FastMCP("PySpark Tools")


@app.tool()
def convert_sql_to_pyspark(
    sql_query: str,
    table_info: Optional[Dict] = None,
    store_result: bool = True
) -> Dict[str, Any]:
    """
    Convert SQL query to PySpark code with optimizations.
    
    Args:
        sql_query: The SQL query to convert
        table_info: Optional table metadata for better optimization
        store_result: Whether to store the result in memory
        
    Returns:
        Dictionary containing PySpark code and optimization suggestions
    """
    try:
        # Check if we have a cached conversion
        cached = memory.get_conversion(sql_query)
        if cached:
            return {
                "status": "success",
                "source": "cache",
                "pyspark_code": cached["pyspark_code"],
                "optimizations": cached["optimization_notes"].split("\n") if cached["optimization_notes"] else [],
                "review_notes": cached["review_notes"]
            }
        
        # Convert SQL to PySpark
        pyspark_code, optimizations = converter.convert_sql_to_pyspark(sql_query, table_info)
        
        # Store result if requested
        if store_result:
            memory.store_conversion(
                sql_query=sql_query,
                pyspark_code=pyspark_code,
                optimization_notes="\n".join(optimizations)
            )
        
        return {
            "status": "success",
            "source": "converted",
            "pyspark_code": pyspark_code,
            "optimizations": optimizations,
            "sql_query": sql_query
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "sql_query": sql_query
        }


@app.tool()
def review_pyspark_code(
    code: str,
    focus_areas: Optional[List[str]] = None
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
        memory.store_context(f"review_{hash(code)}", {
            "code": code,
            "issues": [issue.__dict__ for issue in issues],
            "metrics": metrics,
            "report": report
        })
        
        return {
            "status": "success",
            "issues": [issue.__dict__ for issue in issues],
            "metrics": metrics,
            "report": report,
            "summary": {
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.severity == "error"]),
                "aws_glue_ready": len([i for i in issues if i.category == "aws_glue" and i.severity == "error"]) == 0
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@app.tool()
def optimize_pyspark_code(
    code: str,
    optimization_level: str = "standard"
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
            optimizations.extend([
                "Consider using Delta Lake for ACID transactions",
                "Implement data partitioning strategy",
                "Use column-based file formats (Parquet)",
                "Implement proper error handling and logging",
                "Consider using Glue Data Catalog for metadata management"
            ])
        
        # Remove duplicates
        optimizations = list(set(optimizations))
        
        return {
            "status": "success",
            "optimization_level": optimization_level,
            "suggestions": optimizations,
            "performance_score": max(0, 100 - (metrics["performance_issues"] * 10)),
            "aws_glue_compatibility": len([i for i in issues if i.category == "aws_glue" and i.severity == "error"]) == 0
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


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
            "count": len(conversions)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


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
            "query": query
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


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
        return {
            "status": "success",
            "message": f"Context stored with key: {key}"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


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
            return {
                "status": "success",
                "key": key,
                "value": value
            }
        else:
            return {
                "status": "not_found",
                "key": key,
                "message": "Context key not found"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@app.tool()
def generate_aws_glue_job_template(
    job_name: str,
    source_format: str = "parquet",
    target_format: str = "parquet",
    include_bookmarking: bool = True
) -> Dict[str, Any]:
    """
    Generate an AWS Glue job template with best practices.
    
    Args:
        job_name: Name of the Glue job
        source_format: Source data format (parquet, csv, json, etc.)
        target_format: Target data format
        include_bookmarking: Whether to include job bookmarking
        
    Returns:
        Dictionary containing the Glue job template
    """
    try:
        template = f'''import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_PATH',
    'TARGET_PATH',
    'DATABASE_NAME',
    'TABLE_NAME'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Read source data
    source_df = spark.read.format("{source_format}").load(args['SOURCE_PATH'])
    
    # Apply transformations here
    # Example: transformed_df = source_df.filter(col("status") == "active")
    transformed_df = source_df
    
    # Write to target
    transformed_df.write \\
        .format("{target_format}") \\
        .mode("overwrite") \\
        .save(args['TARGET_PATH'])
    
    # Update Glue Data Catalog if needed
    # glueContext.write_dynamic_frame.from_catalog(
    #     frame=DynamicFrame.fromDF(transformed_df, glueContext, "transformed"),
    #     database=args['DATABASE_NAME'],
    #     table_name=args['TABLE_NAME']
    # )
    
    print(f"Job {{args['JOB_NAME']}} completed successfully")
    
except Exception as e:
    print(f"Job failed with error: {{str(e)}}")
    raise e
    
finally:
    # Commit job for bookmarking
    {"job.commit()" if include_bookmarking else "# Job bookmarking disabled"}
'''
        
        return {
            "status": "success",
            "job_name": job_name,
            "template": template,
            "features": {
                "error_handling": True,
                "parameter_support": True,
                "bookmarking": include_bookmarking,
                "catalog_integration": True
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


if __name__ == "__main__":
    # For development/testing
    app.run(debug=True)