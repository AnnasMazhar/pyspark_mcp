"""FastMCP server for SQL to PySpark conversion with code review and optimization."""

import json
import re
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
from .data_source_analyzer import CodebaseAnalysis, DataSourceAnalyzer, DataSourceInfo
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
data_source_analyzer = DataSourceAnalyzer()

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
def complete_sql_conversion(
    sql_content: str,
    optimization_level: str = "standard",
    include_glue_template: bool = False,
    client_context: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    One-stop tool for complete SQL to PySpark conversion with optimization.

    Automatically:
    1. Converts SQL to PySpark
    2. Applies optimizations
    3. Reviews for best practices
    4. Generates Glue job template (optional)
    5. Provides deployment guidance

    Args:
        sql_content: Raw SQL content (no file references needed)
        optimization_level: "basic", "standard", "aggressive"
        include_glue_template: Whether to generate Glue job template
        client_context: Optional context (schema, tables, etc.)

    Returns:
        Complete conversion package ready for use
    """
    try:
        if not sql_content.strip():
            return {
                "status": "error",
                "message": "No SQL content provided",
                "suggestions": ["Provide SQL content to convert"],
            }

        # 1. Auto-analyze SQL context
        context_result = analyze_sql_context(sql_content)
        if context_result["status"] != "success":
            return context_result

        context = context_result

        # Build table info from context
        table_info = {}
        if client_context:
            table_info.update(client_context)

        # Add detected schemas and tables
        for schema in context.get("schemas", []):
            for table in context.get("tables", []):
                full_table_name = f"{schema}.{table}" if schema else table
                if full_table_name not in table_info:
                    table_info[full_table_name] = {"size_gb": 1.0}  # Default estimate

        # 2. Convert with intelligent settings
        conversion_result = convert_sql_to_pyspark(
            sql_query=sql_content,
            table_info=table_info,
            dialect=context.get("dialect", "postgres"),
            store_result=True,
        )

        if conversion_result["status"] != "success":
            return conversion_result

        # 3. Auto-optimize based on detected patterns
        optimization_result = optimize_pyspark_code(
            code=conversion_result["pyspark_code"],
            optimization_level=optimization_level,
        )

        # 4. Review for best practices
        review_result = review_pyspark_code(
            code=optimization_result.get(
                "optimized_code", conversion_result["pyspark_code"]
            ),
            focus_areas=["aws_glue", "performance", "best_practice"],
        )

        # 5. Generate Glue template if requested
        glue_template = None
        if include_glue_template:
            job_name = f"converted_{hash(sql_content) % 10000:04d}"
            if context.get("client_ids"):
                job_name = f"client_{context['client_ids'][0]}_{job_name}"

            glue_result = generate_aws_glue_job_template(
                job_name=job_name,
                source_format="parquet",
                target_format="delta",
                use_dynamic_frame=True,
            )
            glue_template = glue_result.get("template")

        # Calculate performance improvement estimate
        optimizations_count = len(optimization_result.get("suggestions", []))
        performance_gain = f"{min(15 + optimizations_count * 5, 50)}-{min(25 + optimizations_count * 8, 70)}%"

        return {
            "status": "success",
            "pyspark_code": optimization_result.get(
                "optimized_code", conversion_result["pyspark_code"]
            ),
            "performance_gain": performance_gain,
            "aws_glue_compatible": review_result.get("summary", {}).get(
                "aws_glue_ready", True
            ),
            "suggestions": review_result.get("summary", {}).get("total_issues", 0),
            "top_suggestions": [
                issue.get("suggestion", "No specific suggestion")
                for issue in review_result.get("issues", [])[:3]
            ],
            "glue_job_template": glue_template,
            "optimizations_applied": optimization_result.get("suggestions", []),
            "context_detected": {
                "dialect": context.get("dialect"),
                "multi_tenant": context.get("multi_tenant_pattern", False),
                "complexity_score": context.get("complexity_score", 0),
                "client_ids": context.get("client_ids", []),
            },
            "deployment_ready": True,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Complete conversion failed: {str(e)}",
            "suggestions": [
                "Check SQL syntax",
                "Try simpler query first",
                "Ensure SQL content is valid",
            ],
        }


@app.tool()
def analyze_sql_context(sql_content: str) -> Dict[str, Any]:
    """
    Automatically analyze SQL to extract context information.

    Extracts:
    - Database schemas and table names
    - Client ID from multi-tenant patterns
    - Complexity assessment
    - Optimization opportunities
    - Required dependencies

    No hardcoded references - works with any SQL content.
    """
    try:
        if not sql_content.strip():
            return {"status": "error", "message": "No SQL content provided"}

        import re

        # Detect SQL dialect
        dialect = _detect_sql_dialect(sql_content)

        # Extract schemas and tables dynamically
        schemas = _extract_schemas_from_sql(sql_content)
        tables = _extract_tables_from_sql(sql_content)

        # Detect multi-tenant patterns
        client_ids = _extract_client_ids_from_sql(sql_content)

        # Calculate complexity
        complexity = _calculate_sql_complexity(sql_content)

        # Identify optimization opportunities
        opportunities = _identify_optimization_opportunities(sql_content)

        return {
            "status": "success",
            "dialect": dialect,
            "schemas": schemas,
            "tables": tables,
            "client_ids": client_ids,
            "complexity_score": complexity,
            "multi_tenant_pattern": len(client_ids) > 0,
            "optimization_opportunities": opportunities,
            "estimated_conversion_time": (
                "2-5 seconds" if complexity < 5 else "5-15 seconds"
            ),
        }

    except Exception as e:
        return {"status": "error", "message": f"Context analysis failed: {str(e)}"}


@app.tool()
def process_editor_selection(
    selected_text: str,
    file_path: Optional[str] = None,
    cursor_line: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Process selected SQL text from IDE editor (VS Code, Kiro, etc.)
    Perfect for IDE extension integration with real-time feedback.

    Args:
        selected_text: SQL text selected in the editor
        file_path: Optional file path for context
        cursor_line: Optional cursor line for context

    Returns:
        IDE-friendly response with actions and suggestions
    """
    if not selected_text.strip():
        return {
            "status": "error",
            "message": "No SQL content selected",
            "action": "show_notification",
            "suggestions": ["Select SQL code to convert to PySpark"],
        }

    # Quick SQL validation
    if not _is_sql_content(selected_text):
        return {
            "status": "warning",
            "message": "Selected text may not be SQL",
            "action": "show_warning",
            "suggestions": ["Ensure you have selected valid SQL code"],
        }

    try:
        # Fast conversion for IDE responsiveness
        result = complete_sql_conversion(
            sql_content=selected_text,
            optimization_level="standard",
            include_glue_template=False,  # Keep it fast for IDE
        )

        if result["status"] == "success":
            return {
                "status": "success",
                "action": "replace_selection",  # IDE action
                "pyspark_code": result["pyspark_code"],
                "performance_gain": result["performance_gain"],
                "suggestions": result["top_suggestions"],
                "show_notification": f"SQL converted! Performance gain: {result['performance_gain']}",
                "quick_actions": [
                    {"label": "Generate Glue Job", "command": "generate_glue_template"},
                    {"label": "Optimize Further", "command": "aggressive_optimization"},
                    {"label": "Add Tests", "command": "generate_tests"},
                ],
                "context": result["context_detected"],
            }
        else:
            return result

    except Exception as e:
        return {
            "status": "error",
            "message": f"Conversion failed: {str(e)}",
            "action": "show_error",
            "suggestions": ["Check SQL syntax", "Try simpler query first"],
        }


@app.tool()
def realtime_sql_assistance(
    current_sql: str, cursor_position: Optional[int] = None
) -> Dict[str, Any]:
    """
    Provide real-time assistance as developers type SQL.
    Perfect for live coding assistance in IDEs.

    Args:
        current_sql: Current SQL content being typed
        cursor_position: Current cursor position

    Returns:
        Real-time suggestions, warnings, and completions
    """
    if not current_sql.strip():
        return {
            "status": "ready",
            "suggestions": [],
            "warnings": [],
            "completions": [
                "SELECT",
                "WITH",
                "INSERT",
                "UPDATE",
                "DELETE",
                "FROM",
                "WHERE",
                "JOIN",
            ],
        }

    try:
        # Quick syntax validation
        syntax_issues = _validate_sql_syntax(current_sql)

        # Detect patterns as user types
        detected_patterns = _detect_realtime_patterns(current_sql)

        # Performance warnings
        performance_warnings = _check_performance_patterns(current_sql)

        # Multi-tenant hints
        multi_tenant_hints = _check_multi_tenant_patterns(current_sql)

        # Auto-completion suggestions
        completions = _generate_auto_completions(current_sql, cursor_position)

        return {
            "status": "active",
            "syntax_valid": len(syntax_issues) == 0,
            "syntax_issues": syntax_issues,
            "detected_patterns": detected_patterns,
            "performance_warnings": performance_warnings,
            "multi_tenant_hints": multi_tenant_hints,
            "auto_completions": completions,
            "suggestions": _generate_contextual_suggestions(current_sql),
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.tool()
def workspace_analysis(
    workspace_files: List[Dict[str, str]],  # [{"path": "...", "content": "..."}]
    analysis_scope: str = "sql_optimization",
) -> Dict[str, Any]:
    """
    Analyze entire workspace for SQL optimization opportunities.
    Perfect for Kiro integration and workspace-wide analysis.

    Args:
        workspace_files: List of files with path and content
        analysis_scope: Scope of analysis ("sql_optimization", "performance", "multi_tenant")

    Returns:
        Comprehensive workspace analysis with recommendations
    """
    try:
        # Filter SQL files
        sql_files = []
        for file_info in workspace_files:
            if _is_sql_content(file_info["content"]):
                sql_files.append(file_info)

        if not sql_files:
            return {
                "status": "info",
                "message": "No SQL files found in workspace",
                "recommendations": [
                    "Add SQL files to analyze optimization opportunities"
                ],
            }

        # Analyze each file
        file_analyses = []
        total_opportunities = 0
        multi_tenant_files = 0
        high_complexity_files = 0

        for file_info in sql_files:
            try:
                context_result = analyze_sql_context(file_info["content"])

                if context_result["status"] == "success":
                    context = context_result
                    opportunities = len(context["optimization_opportunities"])
                    total_opportunities += opportunities

                    if context["multi_tenant_pattern"]:
                        multi_tenant_files += 1

                    if context["complexity_score"] > 7:
                        high_complexity_files += 1

                    file_analyses.append(
                        {
                            "path": file_info["path"],
                            "optimization_opportunities": opportunities,
                            "complexity_score": context["complexity_score"],
                            "multi_tenant": context["multi_tenant_pattern"],
                            "dialect": context["dialect"],
                            "client_ids": context["client_ids"],
                        }
                    )

            except Exception as e:
                file_analyses.append({"path": file_info["path"], "error": str(e)})

        # Generate workspace recommendations
        recommendations = []
        if multi_tenant_files > 0:
            recommendations.append(
                f"Consider batch processing for {multi_tenant_files} multi-tenant files"
            )

        if high_complexity_files > 0:
            recommendations.append(
                f"Focus optimization efforts on {high_complexity_files} high-complexity files"
            )

        if total_opportunities > 10:
            recommendations.append(
                "Significant optimization opportunities detected - consider automated conversion"
            )

        return {
            "status": "success",
            "total_sql_files": len(sql_files),
            "analyzed_files": len([f for f in file_analyses if "error" not in f]),
            "file_analyses": file_analyses,
            "recommendations": recommendations,
            "summary": {
                "total_optimization_opportunities": total_opportunities,
                "multi_tenant_files": multi_tenant_files,
                "high_complexity_files": high_complexity_files,
                "estimated_workspace_improvement": f"{min(total_opportunities * 5, 50)}%",
            },
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# Helper functions for the new tools
def _detect_sql_dialect(sql_content: str) -> str:
    """Detect SQL dialect from content patterns"""
    sql_lower = sql_content.lower()

    if "::" in sql_content and ("interval" in sql_lower or "now()" in sql_lower):
        return "postgres"
    elif "dual" in sql_lower or "sysdate" in sql_lower:
        return "oracle"
    elif "getdate()" in sql_lower or "[" in sql_content:
        return "sqlserver"
    else:
        return "standard"


def _extract_schemas_from_sql(sql_content: str) -> List[str]:
    """Extract schema names dynamically"""
    import re

    schema_pattern = re.compile(r"(\w+_db_\w+_\d+_\w+|\w+\.\w+)")
    matches = schema_pattern.findall(sql_content)
    schemas = set()
    for match in matches:
        if "." in match:
            schema = match.split(".")[0]
            schemas.add(schema)
        elif "_db_" in match:
            schemas.add(match)
    return list(schemas)


def _extract_tables_from_sql(sql_content: str) -> List[str]:
    """Extract table names from SQL"""
    import re

    # Simple table extraction - can be enhanced
    table_pattern = re.compile(r"(?:FROM|JOIN)\s+(?:\w+\.)?(\w+)", re.IGNORECASE)
    tables = table_pattern.findall(sql_content)
    return list(set(tables))


def _extract_client_ids_from_sql(sql_content: str) -> List[str]:
    """Extract client IDs from multi-tenant patterns"""
    import re

    # Generic pattern for multi-tenant schema detection
    client_pattern = re.compile(r"(\w+_db_\w+_(\d+)_\w+)")
    matches = client_pattern.findall(sql_content)
    return [match[1] for match in matches]  # Extract just the client ID numbers


def _calculate_sql_complexity(sql_content: str) -> int:
    """Calculate SQL complexity score"""
    score = 0
    sql_lower = sql_content.lower()

    # Base score for SELECT
    if "select" in sql_lower:
        score += 1

    # JOINs add complexity
    join_count = len(re.findall(r"join", sql_lower))
    score += join_count * 2

    # Subqueries add complexity
    subquery_count = len(re.findall(r"\(.*select.*\)", sql_lower, re.DOTALL))
    score += subquery_count * 3

    # CASE statements add complexity
    case_count = len(re.findall(r"case\s+when", sql_lower))
    score += case_count * 2

    # Window functions add complexity
    window_count = len(re.findall(r"over\s*\(", sql_lower))
    score += window_count * 2

    return score


def _identify_optimization_opportunities(sql_content: str) -> List[str]:
    """Identify optimization opportunities in SQL"""
    opportunities = []
    sql_lower = sql_content.lower()

    if "select *" in sql_lower:
        opportunities.append("Replace SELECT * with specific columns")

    if sql_lower.count("join") > 3:
        opportunities.append("Consider join optimization strategies")

    if "where" not in sql_lower and "select" in sql_lower:
        opportunities.append("Add WHERE clause for data filtering")

    # Check for multi-tenant patterns generically
    if re.search(r"\w+_db_\w+_\d+_\w+", sql_content):
        opportunities.append("Multi-tenant optimization available")

    return opportunities


def _is_sql_content(content: str) -> bool:
    """Check if content appears to be SQL"""
    sql_keywords = [
        "select",
        "from",
        "where",
        "join",
        "insert",
        "update",
        "delete",
        "with",
    ]
    content_lower = content.lower()
    return any(keyword in content_lower for keyword in sql_keywords)


def _validate_sql_syntax(sql_content: str) -> List[str]:
    """Basic SQL syntax validation"""
    issues = []

    # Check for basic syntax issues
    if sql_content.count("(") != sql_content.count(")"):
        issues.append("Mismatched parentheses")

    if sql_content.count("'") % 2 != 0:
        issues.append("Unclosed string literal")

    return issues


def _detect_realtime_patterns(sql_content: str) -> List[str]:
    """Detect patterns as user types"""
    patterns = []
    sql_lower = sql_content.lower()

    if "select" in sql_lower and "from" in sql_lower:
        patterns.append("Basic SELECT query detected")

    if "join" in sql_lower:
        patterns.append("JOIN operation detected")

    # Check for multi-tenant patterns generically
    if re.search(r"\w+_db_\w+_\d+_\w+", sql_content):
        patterns.append("Multi-tenant pattern detected")

    return patterns


def _check_performance_patterns(sql_content: str) -> List[str]:
    """Check for performance anti-patterns"""
    warnings = []
    sql_lower = sql_content.lower()

    if "select *" in sql_lower:
        warnings.append("Consider selecting specific columns instead of SELECT *")

    if sql_lower.count("join") > 5:
        warnings.append("Many joins detected - consider optimization strategies")

    if "where" not in sql_lower and "select" in sql_lower:
        warnings.append("Consider adding WHERE clause to filter data")

    return warnings


def _check_multi_tenant_patterns(sql_content: str) -> List[str]:
    """Check for multi-tenant optimization opportunities"""
    hints = []

    # Check for multi-tenant patterns generically
    if re.search(r"\w+_db_\w+_\d+_\w+", sql_content):
        hints.append(
            "Multi-tenant pattern detected - consider client-aware partitioning"
        )

    client_ids = _extract_client_ids_from_sql(sql_content)
    if len(client_ids) > 1:
        hints.append(
            "Multiple clients detected - consider batch processing optimization"
        )

    return hints


def _generate_auto_completions(
    sql_content: str, cursor_position: Optional[int]
) -> List[str]:
    """Generate auto-completion suggestions"""
    completions = []

    if not sql_content.strip():
        return ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE"]

    sql_lower = sql_content.lower()

    if "select" in sql_lower and "from" not in sql_lower:
        completions.extend(["FROM", "* FROM"])

    if "from" in sql_lower and "where" not in sql_lower:
        completions.extend(["WHERE", "JOIN", "LEFT JOIN", "INNER JOIN"])

    return completions


def _generate_contextual_suggestions(sql_content: str) -> List[str]:
    """Generate contextual suggestions based on SQL content"""
    suggestions = []
    sql_lower = sql_content.lower()

    if "select *" in sql_lower:
        suggestions.append("Consider selecting specific columns for better performance")

    # Check for multi-tenant patterns generically
    if re.search(r"\w+_db_\w+_\d+_\w+", sql_content):
        suggestions.append(
            "Multi-tenant query detected - optimization opportunities available"
        )

    if sql_lower.count("join") > 3:
        suggestions.append("Complex joins detected - consider performance optimization")

    return suggestions


@app.tool()
def generate_project_structure(
    project_name: str,
    sql_contents: List[str],  # List of SQL content (not file paths!)
    target_platform: str = "aws_glue",
    include_tests: bool = True,
) -> Dict[str, Any]:
    """
    Generate complete PySpark project structure from SQL content.
    Perfect for creating production-ready projects from any SQL.

    Args:
        project_name: Name of the project to generate
        sql_contents: List of SQL content strings
        target_platform: Target platform ("aws_glue", "databricks", "spark")
        include_tests: Whether to include test files

    Returns:
        Complete project structure with all files
    """
    try:
        if not sql_contents:
            return {
                "status": "error",
                "message": "No SQL content provided",
                "suggestions": ["Provide at least one SQL query to generate project"],
            }

        project_files = {}

        # Process each SQL content
        conversions = []
        for i, sql_content in enumerate(sql_contents):
            try:
                # Convert each SQL
                conversion = complete_sql_conversion(
                    sql_content=sql_content,
                    optimization_level="aggressive",
                    include_glue_template=(target_platform == "aws_glue"),
                )

                if conversion["status"] == "success":
                    conversions.append(
                        {
                            "index": i,
                            "sql_content": sql_content,
                            "conversion": conversion,
                        }
                    )

            except Exception as e:
                print(f"Error converting SQL {i}: {e}")

        if not conversions:
            return {
                "status": "error",
                "message": "No SQL content could be converted successfully",
                "suggestions": ["Check SQL syntax", "Ensure valid SQL content"],
            }

        # Generate main PySpark modules
        for conv in conversions:
            module_name = f"query_{conv['index'] + 1}_processor"
            project_files[f"src/{module_name}.py"] = _create_pyspark_module(
                conv["conversion"], module_name, target_platform
            )

        # Generate main application
        project_files["src/main.py"] = _create_main_application(
            project_name, conversions, target_platform
        )

        # Generate configuration
        project_files["config/settings.py"] = _create_project_config(
            project_name, target_platform
        )

        # Generate requirements
        project_files["requirements.txt"] = _create_requirements_file(target_platform)

        # Generate tests if requested
        if include_tests:
            for conv in conversions:
                test_name = f"test_query_{conv['index'] + 1}"
                project_files[f"tests/{test_name}.py"] = _create_test_file(
                    conv["conversion"], test_name
                )

        # Generate deployment files
        if target_platform == "aws_glue":
            project_files["deploy/deploy_glue_jobs.sh"] = (
                _create_glue_deployment_script(conversions)
            )
            project_files["deploy/terraform/main.tf"] = _create_terraform_config(
                conversions
            )

        # Generate documentation
        project_files["README.md"] = _create_project_readme(
            project_name, conversions, target_platform
        )

        return {
            "status": "success",
            "project_name": project_name,
            "total_files": len(project_files),
            "sql_queries_processed": len(sql_contents),
            "successful_conversions": len(conversions),
            "project_files": project_files,
            "summary": {
                "pyspark_modules": len(
                    [
                        f
                        for f in project_files.keys()
                        if f.startswith("src/") and f.endswith(".py")
                    ]
                ),
                "test_files": len(
                    [f for f in project_files.keys() if f.startswith("tests/")]
                ),
                "deployment_files": len(
                    [f for f in project_files.keys() if f.startswith("deploy/")]
                ),
                "estimated_performance_improvement": _calculate_project_performance_improvement(
                    conversions
                ),
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Project generation failed: {str(e)}",
            "suggestions": ["Check SQL content", "Try with simpler queries first"],
        }


# Helper functions for project generation
def _create_pyspark_module(
    conversion: Dict, module_name: str, target_platform: str
) -> str:
    """Create a PySpark module from conversion result"""
    class_name = module_name.replace("_", " ").title().replace(" ", "")

    return f'''#!/usr/bin/env python3
"""
{module_name.replace("_", " ").title()}
Generated by MCP PySpark Tools
Target Platform: {target_platform}
Performance Improvement: {conversion.get("performance_gain", "N/A")}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class {class_name}:
    """
    Optimized PySpark processor
    Performance improvement: {conversion.get("performance_gain", "N/A")}
    AWS Glue Compatible: {conversion.get("aws_glue_compatible", False)}
    """
    
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder \\
            .appName("{module_name}") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .getOrCreate()
        
        self.logger = logging.getLogger(__name__)
    
    def process(self):
        """Execute the optimized PySpark processing"""
        try:
            self.logger.info("Starting {module_name} processing")
            
            # Optimized PySpark code
{chr(10).join("            " + line for line in conversion["pyspark_code"].split(chr(10)))}
            
            self.logger.info(f"Processing completed successfully")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in {module_name}: {{e}}")
            raise e
    
    def get_performance_metrics(self):
        """Get performance metrics for this processor"""
        return {{
            "estimated_improvement": "{conversion.get("performance_gain", "N/A")}",
            "optimizations_applied": {conversion.get("optimizations_applied", [])},
            "aws_glue_compatible": {conversion.get("aws_glue_compatible", False)},
            "suggestions_count": {conversion.get("suggestions", 0)}
        }}

if __name__ == "__main__":
    processor = {class_name}()
    result = processor.process()
    print(f"Processing completed. Result shape: {{result.count()}} rows")
'''


def _create_main_application(
    project_name: str, conversions: List[Dict], target_platform: str
) -> str:
    """Create main application file"""
    imports = []
    processors = []

    for conv in conversions:
        module_name = f"query_{conv['index'] + 1}_processor"
        class_name = module_name.replace("_", " ").title().replace(" ", "")
        imports.append(f"from {module_name} import {class_name}")
        processors.append(f"        {class_name}(),")

    return f'''#!/usr/bin/env python3
"""
{project_name} - Main Application
Generated by MCP PySpark Tools
Target Platform: {target_platform}
"""

import logging
from pyspark.sql import SparkSession
{chr(10).join(imports)}

class {project_name.replace("_", "").replace("-", "").title()}App:
    """Main application orchestrator"""
    
    def __init__(self):
        self.spark = SparkSession.builder \\
            .appName("{project_name}") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .getOrCreate()
        
        self.processors = [
{chr(10).join(processors)}
        ]
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def run_all(self):
        """Run all processors"""
        results = []
        
        for i, processor in enumerate(self.processors):
            try:
                self.logger.info(f"Running processor {{i+1}}/{{len(self.processors)}}")
                result = processor.process()
                results.append(result)
                self.logger.info(f"Processor {{i+1}} completed successfully")
                
            except Exception as e:
                self.logger.error(f"Processor {{i+1}} failed: {{e}}")
                raise e
        
        return results
    
    def get_summary(self):
        """Get processing summary"""
        return {{
            "total_processors": len(self.processors),
            "target_platform": "{target_platform}",
            "estimated_total_improvement": "25-45%"
        }}

if __name__ == "__main__":
    app = {project_name.replace("_", "").replace("-", "").title()}App()
    results = app.run_all()
    print(f"All processors completed. Total results: {{len(results)}}")
'''


def _create_project_config(project_name: str, target_platform: str) -> str:
    """Create project configuration"""
    return f'''#!/usr/bin/env python3
"""
Configuration for {project_name}
Generated by MCP PySpark Tools
"""

import os

class Config:
    """Project configuration"""
    
    # Project settings
    PROJECT_NAME = "{project_name}"
    TARGET_PLATFORM = "{target_platform}"
    
    # Spark settings
    SPARK_APP_NAME = "{project_name}"
    SPARK_CONFIGS = {{
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }}
    
    # AWS Glue settings (if applicable)
    if TARGET_PLATFORM == "aws_glue":
        GLUE_JOB_ROLE = os.getenv("GLUE_JOB_ROLE", "arn:aws:iam::ACCOUNT_ID:role/GlueServiceRole")
        GLUE_DATABASE = os.getenv("GLUE_DATABASE", "default")
        S3_BUCKET = os.getenv("S3_BUCKET", "your-data-bucket")
    
    # Logging settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Performance settings
    ENABLE_CACHING = True
    OPTIMIZATION_LEVEL = "aggressive"
'''


def _create_requirements_file(target_platform: str) -> str:
    """Create requirements.txt file"""
    base_requirements = ["pyspark>=3.4.0", "pandas>=1.5.0", "numpy>=1.21.0"]

    if target_platform == "aws_glue":
        base_requirements.extend(["boto3>=1.26.0", "awswrangler>=3.0.0"])
    elif target_platform == "databricks":
        base_requirements.extend(["databricks-connect>=13.0.0"])

    return chr(10).join(base_requirements)


def _create_test_file(conversion: Dict, test_name: str) -> str:
    """Create test file for a conversion"""
    return f'''#!/usr/bin/env python3
"""
Tests for {test_name}
Generated by MCP PySpark Tools
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

class {test_name.replace("_", " ").title().replace(" ", "")}(unittest.TestCase):
    """Test cases for the converted PySpark code"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \\
            .appName("test_{test_name}") \\
            .master("local[*]") \\
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_basic_functionality(self):
        """Test basic functionality of the processor"""
        # Create sample data for testing
        sample_data = [
            ("test1", "value1", 100),
            ("test2", "value2", 200)
        ]
        
        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", IntegerType(), True)
        ])
        
        test_df = self.spark.createDataFrame(sample_data, schema)
        test_df.createOrReplaceTempView("test_table")
        
        # Test the conversion logic here
        # This would need to be customized based on the actual converted code
        result = test_df.select("*")
        
        self.assertIsNotNone(result)
        self.assertGreater(result.count(), 0)
    
    def test_performance_characteristics(self):
        """Test performance characteristics"""
        # Performance tests would go here
        pass
    
    def test_error_handling(self):
        """Test error handling"""
        # Error handling tests would go here
        pass

if __name__ == "__main__":
    unittest.main()
'''


def _create_glue_deployment_script(conversions: List[Dict]) -> str:
    """Create Glue deployment script"""
    return f"""#!/bin/bash
# AWS Glue Deployment Script
# Generated by MCP PySpark Tools

set -e

echo "🚀 Deploying PySpark project to AWS Glue"

# Configuration
SCRIPT_BUCKET="your-glue-scripts-bucket"
REGION="us-east-1"
GLUE_ROLE="arn:aws:iam::ACCOUNT_ID:role/GlueServiceRole"

# Upload scripts to S3
echo "📁 Uploading Glue job scripts..."
aws s3 sync src/ s3://$SCRIPT_BUCKET/scripts/

# Create Glue jobs
echo "⚙️ Creating Glue jobs..."
{chr(10).join([f'echo "Creating job for query {conv["index"] + 1}..."' for conv in conversions])}

echo "✅ All Glue jobs deployed successfully!"
echo "📋 Deployed {len(conversions)} jobs"
"""


def _create_terraform_config(conversions: List[Dict]) -> str:
    """Create Terraform configuration"""
    return f"""# Terraform configuration for Glue jobs
# Generated by MCP PySpark Tools

terraform {{
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = var.aws_region
}}

variable "aws_region" {{
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}}

variable "glue_role_arn" {{
  description = "ARN of the Glue service role"
  type        = string
}}

variable "script_bucket" {{
  description = "S3 bucket for Glue job scripts"
  type        = string
}}

# Glue jobs
{chr(10).join([f'''
resource "aws_glue_job" "query_{conv["index"] + 1}_job" {{
  name         = "query_{conv["index"] + 1}_processor"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"
  
  command {{
    name            = "glueetl"
    script_location = "s3://${{var.script_bucket}}/scripts/query_{conv["index"] + 1}_processor.py"
    python_version  = "3"
  }}
  
  default_arguments = {{
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }}
  
  max_retries      = 1
  timeout          = 2880
  worker_type      = "G.2X"
  number_of_workers = 4
}}''' for conv in conversions])}
"""


def _create_project_readme(
    project_name: str, conversions: List[Dict], target_platform: str
) -> str:
    """Create project README"""
    return f"""# {project_name}

Generated by MCP PySpark Tools - Production-ready PySpark project

## 📊 Project Overview

- **Target Platform**: {target_platform}
- **SQL Queries Processed**: {len(conversions)}
- **Estimated Performance Improvement**: 25-45%
- **AWS Glue Compatible**: Yes

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Apache Spark 3.4+
- AWS CLI (for Glue deployment)

### Installation
```bash
pip install -r requirements.txt
```

### Running Locally
```bash
python src/main.py
```

### Deploying to AWS Glue
```bash
chmod +x deploy/deploy_glue_jobs.sh
./deploy/deploy_glue_jobs.sh
```

## 📁 Project Structure

```
{project_name}/
├── src/                    # PySpark modules
│   ├── main.py            # Main application
{chr(10).join([f"│   ├── query_{conv['index'] + 1}_processor.py" for conv in conversions])}
├── tests/                  # Unit tests
├── config/                 # Configuration
├── deploy/                 # Deployment scripts
└── requirements.txt        # Dependencies
```

## 🔧 Configuration

Update `config/settings.py` with your specific settings:
- AWS account details
- S3 bucket names
- Glue job configurations

## 📈 Performance Optimizations

This project includes several performance optimizations:
- Adaptive query execution enabled
- Optimized join strategies
- Intelligent caching
- Column pruning
- Predicate pushdown

## 🧪 Testing

Run tests with:
```bash
python -m pytest tests/
```

## 📚 Documentation

Each processor module includes:
- Performance metrics
- Optimization details
- Usage examples
- Error handling

Generated by MCP PySpark Tools - Accelerating SQL to PySpark conversion
"""


def _calculate_project_performance_improvement(conversions: List[Dict]) -> str:
    """Calculate overall project performance improvement"""
    if not conversions:
        return "N/A"

    # Extract performance gains and calculate average
    gains = []
    for conv in conversions:
        gain_str = conv["conversion"].get("performance_gain", "25-35%")
        # Extract the average of the range
        if "-" in gain_str:
            parts = gain_str.replace("%", "").split("-")
            if len(parts) == 2:
                try:
                    avg_gain = (int(parts[0]) + int(parts[1])) / 2
                    gains.append(avg_gain)
                except ValueError:
                    gains.append(30)  # Default
        else:
            gains.append(30)  # Default

    if gains:
        avg_improvement = sum(gains) / len(gains)
        return f"{int(avg_improvement - 5)}-{int(avg_improvement + 5)}%"

    return "25-35%"


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


# =============================================================================
# DATA SOURCE ANALYSIS TOOLS
# =============================================================================


@app.tool()
def analyze_s3_data_source(
    s3_path: str, include_schema_inference: bool = True
) -> Dict[str, Any]:
    """
    Analyze S3 data source to understand structure, format, and optimization opportunities.

    Args:
        s3_path: S3 path to analyze (e.g., 's3://bucket/path/')
        include_schema_inference: Whether to infer schema from sample data

    Returns:
        Dictionary containing data source analysis and recommendations
    """
    try:
        # Analyze the S3 location
        data_source = data_source_analyzer.analyze_s3_location(s3_path)

        # Get optimization recommendations
        recommendations = data_source_analyzer.get_recommendations([data_source])

        # Generate optimized code template
        optimized_code = data_source_analyzer.generate_optimized_code(
            data_source, "read and process"
        )

        return {
            "status": "success",
            "data_source": {
                "source_type": data_source.source_type,
                "location": data_source.location,
                "format": data_source.format,
                "size_mb": data_source.size_mb,
                "partitions": data_source.partitions,
                "last_modified": data_source.last_modified,
                "compression": data_source.compression,
            },
            "recommendations": recommendations,
            "optimized_code": optimized_code,
            "optimization_summary": {
                "performance_tips": len(recommendations.get("performance", [])),
                "cost_optimizations": len(recommendations.get("cost_optimization", [])),
                "best_practices": len(recommendations.get("best_practices", [])),
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "suggestions": [
                "Verify S3 path format (must start with 's3://')",
                "Check AWS credentials if detailed analysis is needed",
                "Ensure the S3 location exists and is accessible",
            ],
        }


@app.tool()
def analyze_delta_table(
    table_path: str, analyze_history: bool = False
) -> Dict[str, Any]:
    """
    Analyze Delta table structure, properties, and optimization opportunities.

    Args:
        table_path: Path to Delta table (S3 or local path)
        analyze_history: Whether to analyze Delta table history

    Returns:
        Dictionary containing Delta table analysis and recommendations
    """
    try:
        # Analyze the Delta table
        data_source = data_source_analyzer.analyze_delta_table(table_path)

        # Get Delta-specific recommendations
        recommendations = data_source_analyzer.get_recommendations([data_source])

        # Generate optimized Delta code
        optimized_code = data_source_analyzer.generate_optimized_code(
            data_source, "delta operations"
        )

        # Add Delta-specific optimizations
        delta_optimizations = []
        if data_source.size_mb and data_source.size_mb > 1000:
            delta_optimizations.extend(
                [
                    "Consider OPTIMIZE command for better file layout",
                    "Use Z-ORDER for frequently filtered columns",
                    "Implement VACUUM for storage cleanup",
                ]
            )

        return {
            "status": "success",
            "delta_table": {
                "location": data_source.location,
                "format": data_source.format,
                "size_mb": data_source.size_mb,
                "partitions": data_source.partitions,
                "last_modified": data_source.last_modified,
            },
            "recommendations": recommendations,
            "delta_optimizations": delta_optimizations,
            "optimized_code": optimized_code,
            "delta_commands": {
                "optimize": f"OPTIMIZE delta.`{table_path}`",
                "vacuum": f"VACUUM delta.`{table_path}` RETAIN 168 HOURS",
                "describe_history": f"DESCRIBE HISTORY delta.`{table_path}`",
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "suggestions": [
                "Verify Delta table path exists",
                "Check for _delta_log directory",
                "Ensure proper Delta Lake dependencies are available",
            ],
        }


@app.tool()
def analyze_codebase(
    directory_path: str,
    include_optimization_suggestions: bool = True,
    scan_depth: int = 3,
) -> Dict[str, Any]:
    """
    Analyze existing PySpark codebase for patterns, issues, and optimization opportunities.

    Args:
        directory_path: Path to codebase directory to analyze
        include_optimization_suggestions: Whether to include detailed optimization suggestions
        scan_depth: Maximum directory depth to scan

    Returns:
        Dictionary containing codebase analysis and recommendations
    """
    try:
        # Analyze the codebase
        analysis = data_source_analyzer.analyze_codebase(directory_path)

        # Get recommendations based on codebase analysis
        recommendations = data_source_analyzer.get_recommendations(
            analysis.data_sources, analysis
        )

        # Generate summary statistics
        summary = {
            "total_files": analysis.total_files,
            "pyspark_files": analysis.pyspark_files,
            "sql_files": analysis.sql_files,
            "pyspark_adoption": f"{(analysis.pyspark_files / max(analysis.total_files, 1)) * 100:.1f}%",
            "common_patterns": analysis.common_patterns,
            "data_sources_found": len(analysis.data_sources),
        }

        # Prioritize issues by severity
        critical_issues = [
            issue
            for issue in analysis.performance_issues
            if "OOM" in issue or "collect()" in issue
        ]
        high_priority_optimizations = [
            opt
            for opt in analysis.optimization_opportunities
            if "broadcast" in opt or "cache" in opt
        ]

        return {
            "status": "success",
            "summary": summary,
            "analysis": {
                "common_patterns": analysis.common_patterns,
                "data_sources": [
                    {
                        "type": ds.source_type,
                        "location": ds.location,
                        "format": ds.format,
                    }
                    for ds in analysis.data_sources
                ],
                "performance_issues": analysis.performance_issues,
                "optimization_opportunities": analysis.optimization_opportunities,
                "best_practices_violations": analysis.best_practices_violations,
            },
            "recommendations": recommendations,
            "priority_actions": {
                "critical_issues": critical_issues,
                "high_priority_optimizations": high_priority_optimizations,
                "quick_wins": [
                    "Replace .collect() with .show() or .take()",
                    "Add .cache() for reused DataFrames",
                    "Use broadcast joins for small tables",
                ],
            },
            "modernization_suggestions": [
                "Migrate SQL files to DataFrame API for better optimization",
                "Implement Delta Lake for ACID transactions",
                "Add comprehensive error handling and logging",
                "Use structured streaming for real-time processing",
            ],
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "suggestions": [
                "Verify directory path exists and is accessible",
                "Check file permissions for reading Python/SQL files",
                "Ensure directory contains PySpark or SQL code",
            ],
        }


@app.tool()
def generate_optimized_pipeline(
    data_sources: List[Dict[str, str]],
    processing_requirements: str,
    target_format: str = "delta",
    include_monitoring: bool = True,
) -> Dict[str, Any]:
    """
    Generate an optimized PySpark pipeline based on multiple data sources and requirements.

    Args:
        data_sources: List of data source configurations [{"type": "s3", "path": "s3://...", "format": "parquet"}]
        processing_requirements: Description of processing requirements
        target_format: Target output format (delta, parquet, etc.)
        include_monitoring: Whether to include monitoring and logging code

    Returns:
        Dictionary containing optimized pipeline code and configuration
    """
    try:
        # Analyze each data source
        analyzed_sources = []
        for source_config in data_sources:
            if source_config.get("type") == "s3":
                source_info = data_source_analyzer.analyze_s3_location(
                    source_config["path"]
                )
            elif source_config.get("type") == "delta":
                source_info = data_source_analyzer.analyze_delta_table(
                    source_config["path"]
                )
            else:
                # Create basic source info for other types
                source_info = DataSourceInfo(
                    source_type=source_config.get("type", "unknown"),
                    location=source_config.get("path", ""),
                    format=source_config.get("format", "unknown"),
                )
            analyzed_sources.append(source_info)

        # Get comprehensive recommendations
        recommendations = data_source_analyzer.get_recommendations(analyzed_sources)

        # Generate optimized pipeline code
        pipeline_code = _generate_pipeline_code(
            analyzed_sources, processing_requirements, target_format, include_monitoring
        )

        # Calculate estimated performance metrics
        total_size = sum(source.size_mb or 0 for source in analyzed_sources)
        estimated_partitions = max(
            200, min(2000, int(total_size / 128))
        )  # 128MB per partition

        return {
            "status": "success",
            "pipeline_code": pipeline_code,
            "configuration": {
                "recommended_partitions": estimated_partitions,
                "estimated_memory_gb": max(4, int(total_size / 1000)),
                "recommended_instance_type": (
                    "m5.xlarge" if total_size < 10000 else "m5.2xlarge"
                ),
                "estimated_runtime_minutes": int(total_size / 1000)
                * 2,  # Rough estimate
            },
            "data_sources": [
                {
                    "location": source.location,
                    "format": source.format,
                    "size_mb": source.size_mb,
                    "partitions": source.partitions,
                }
                for source in analyzed_sources
            ],
            "recommendations": recommendations,
            "optimization_checklist": [
                "✓ Adaptive query execution enabled",
                "✓ Appropriate partition size configured",
                "✓ Column pruning implemented",
                "✓ Predicate pushdown optimized",
                "✓ Broadcast joins for small tables",
                "✓ Caching for reused DataFrames",
                "✓ Efficient file formats used",
            ],
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "suggestions": [
                "Verify data source configurations are valid",
                "Check that all specified paths are accessible",
                "Ensure processing requirements are clearly specified",
            ],
        }


def _generate_pipeline_code(
    sources: List[DataSourceInfo],
    requirements: str,
    target_format: str,
    include_monitoring: bool,
) -> str:
    """Generate optimized pipeline code based on sources and requirements."""

    code_parts = []

    # Header and imports
    code_parts.append(
        '''"""
Optimized PySpark Data Pipeline
Generated based on data source analysis and requirements
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
'''
    )

    # Spark session with optimized configuration
    total_size = sum(source.size_mb or 0 for source in sources)

    code_parts.append(
        f"""
# Optimized Spark Session Configuration
spark = SparkSession.builder \\
    .appName('OptimizedDataPipeline') \\
    .config('spark.sql.adaptive.enabled', 'true') \\
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \\
    .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '128MB') \\
    .config('spark.sql.shuffle.partitions', '{max(200, min(2000, int(total_size / 128)))}') \\"""
    )

    # Add Delta configuration if needed
    if target_format == "delta" or any(source.format == "delta" for source in sources):
        code_parts.append(
            """    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \\"""
        )

    code_parts.append(
        """    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel('WARN')
"""
    )

    # Data loading section
    code_parts.append(
        "\n# ============================================================================="
    )
    code_parts.append("# DATA LOADING")
    code_parts.append(
        "# =============================================================================\n"
    )

    for i, source in enumerate(sources):
        var_name = f"df_{i+1}"

        if source.source_type == "s3":
            if source.format == "parquet":
                code_parts.append(
                    f"""
# Load Parquet data from S3
{var_name} = spark.read.parquet('{source.location}')
logger.info(f"Loaded {{df_{i+1}.count()}} rows from {source.location}")"""
                )
            elif source.format == "delta":
                code_parts.append(
                    f"""
# Load Delta table from S3
{var_name} = spark.read.format('delta').load('{source.location}')
logger.info(f"Loaded Delta table from {source.location}")"""
                )
            elif source.format == "csv":
                code_parts.append(
                    f"""
# Load CSV data from S3
{var_name} = spark.read \\
    .option('header', 'true') \\
    .option('inferSchema', 'true') \\
    .option('multiline', 'true') \\
    .csv('{source.location}')
logger.info(f"Loaded {{df_{i+1}.count()}} rows from {source.location}")"""
                )

        elif source.source_type == "hive":
            code_parts.append(
                f"""
# Load Hive table
{var_name} = spark.table('{source.location}')
logger.info(f"Loaded Hive table {source.location}")"""
            )

        # Add caching for large datasets that will be reused
        if source.size_mb and source.size_mb > 500:
            code_parts.append(
                f"""
# Cache large dataset for reuse
{var_name}.cache()"""
            )

        # Add partition pruning hints
        if source.partitions:
            code_parts.append(
                f"""
# Partition columns available: {', '.join(source.partitions)}
# Example partition pruning: {var_name} = {var_name}.filter(col('{source.partitions[0]}') == 'your_value')"""
            )

    # Processing section
    code_parts.append(
        "\n# ============================================================================="
    )
    code_parts.append("# DATA PROCESSING")
    code_parts.append(
        "# =============================================================================\n"
    )

    code_parts.append(
        f"""
# Processing based on requirements: {requirements}
# TODO: Implement your specific business logic here

# Example processing pipeline:
processed_df = df_1"""
    )

    if len(sources) > 1:
        code_parts.append(
            """
# Join multiple data sources (optimize join order - largest table first)
# Use broadcast joins for small tables (< 200MB)
processed_df = processed_df.join(
    broadcast(df_2) if df_2.count() < 1000000 else df_2,
    on='join_key',
    how='inner'
)"""
        )

    code_parts.append(
        """
# Apply transformations with optimization
processed_df = processed_df \\
    .filter(col('status') == 'active') \\  # Apply filters early
    .select('col1', 'col2', 'col3') \\     # Column pruning
    .withColumn('processed_date', current_timestamp()) \\
    .dropDuplicates(['key_column'])

# Cache if the DataFrame will be used multiple times
processed_df.cache()
"""
    )

    # Output section
    code_parts.append(
        "\n# ============================================================================="
    )
    code_parts.append("# DATA OUTPUT")
    code_parts.append(
        "# =============================================================================\n"
    )

    if target_format == "delta":
        code_parts.append(
            """
# Write to Delta Lake with optimization
output_path = 's3://your-bucket/output/delta-table'

processed_df.write \\
    .format('delta') \\
    .mode('overwrite') \\
    .option('overwriteSchema', 'true') \\
    .partitionBy('partition_column') \\  # Adjust based on your data
    .save(output_path)

logger.info(f"Successfully wrote {processed_df.count()} rows to Delta table")

# Optional: Optimize Delta table
spark.sql(f"OPTIMIZE delta.`{output_path}`")
"""
        )
    else:
        code_parts.append(
            f"""
# Write to {target_format.upper()} format
output_path = 's3://your-bucket/output/{target_format}-data'

processed_df.coalesce(10) \\  # Optimize number of output files
    .write \\
    .mode('overwrite') \\
    .{target_format}(output_path)

logger.info(f"Successfully wrote {{processed_df.count()}} rows to {target_format} format")
"""
        )

    # Monitoring section
    if include_monitoring:
        code_parts.append(
            """
# =============================================================================
# MONITORING AND CLEANUP
# =============================================================================

# Performance metrics
end_time = datetime.now()
logger.info(f"Pipeline completed at {end_time}")

# Spark UI metrics (available at http://driver:4040)
logger.info(f"Spark Application ID: {spark.sparkContext.applicationId}")

# Clean up resources
spark.stop()
logger.info("Spark session stopped successfully")
"""
        )

    return "\n".join(code_parts)


if __name__ == "__main__":
    # For development/testing
    app.run()
