"""
Data Source Analyzer for S3, Delta Tables, and existing PySpark codebases.
Provides intelligent recommendations based on actual data sources and patterns.
"""

import ast
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception
    BOTO3_AVAILABLE = False


@dataclass
class DataSourceInfo:
    """Information about a data source."""

    source_type: str  # 's3', 'delta', 'hive', 'jdbc'
    location: str
    format: str  # 'parquet', 'delta', 'csv', 'json'
    schema: Optional[Dict] = None
    partitions: Optional[List[str]] = None
    size_mb: Optional[float] = None
    row_count: Optional[int] = None
    last_modified: Optional[str] = None
    compression: Optional[str] = None


@dataclass
class CodebaseAnalysis:
    """Analysis results of existing PySpark codebase."""

    total_files: int
    pyspark_files: int
    sql_files: int
    common_patterns: List[str]
    data_sources: List[DataSourceInfo]
    optimization_opportunities: List[str]
    performance_issues: List[str]
    best_practices_violations: List[str]


class DataSourceAnalyzer:
    """Analyzes data sources and existing codebases for optimization recommendations."""

    def __init__(self):
        self.s3_client = None
        self.glue_client = None
        self._initialize_aws_clients()

    def _initialize_aws_clients(self):
        """Initialize AWS clients if credentials are available."""
        if not BOTO3_AVAILABLE:
            return

        try:
            self.s3_client = boto3.client("s3")
            self.glue_client = boto3.client("glue")
        except (NoCredentialsError, Exception):
            # AWS clients will be None if no credentials
            pass

    def analyze_s3_location(self, s3_path: str) -> DataSourceInfo:
        """Analyze S3 location to determine format, size, and structure."""
        try:
            # Parse S3 path
            if not s3_path.startswith("s3://"):
                raise ValueError("S3 path must start with 's3://'")

            path_parts = s3_path[5:].split("/", 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ""

            if not self.s3_client:
                # Return basic info without AWS access
                return DataSourceInfo(
                    source_type="s3",
                    location=s3_path,
                    format=self._infer_format_from_path(s3_path),
                    schema=None,
                    size_mb=None,
                    row_count=None,
                )

            # List objects to analyze structure
            response = self.s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, MaxKeys=100
            )

            if "Contents" not in response:
                raise ValueError(f"No objects found at {s3_path}")

            objects = response["Contents"]
            total_size = sum(obj["Size"] for obj in objects)

            # Infer format from file extensions
            formats = set()
            for obj in objects:
                key = obj["Key"]
                if "." in key:
                    ext = key.split(".")[-1].lower()
                    if ext in ["parquet", "csv", "json", "orc", "avro"]:
                        formats.add(ext)
                # Also check for parquet files without extension but in parquet-like paths
                elif "parquet" in key.lower():
                    formats.add("parquet")

            # Detect partitioning
            partitions = self._detect_partitions(objects)

            # Detect Delta table
            is_delta = any("_delta_log" in obj["Key"] for obj in objects)
            format_type = (
                "delta" if is_delta else (list(formats)[0] if formats else "unknown")
            )

            return DataSourceInfo(
                source_type="s3",
                location=s3_path,
                format=format_type,
                partitions=partitions,
                size_mb=total_size / (1024 * 1024),
                last_modified=max(obj["LastModified"] for obj in objects).isoformat(),
            )

        except Exception as e:
            return DataSourceInfo(
                source_type="s3",
                location=s3_path,
                format="unknown",
                schema={"error": str(e)},
            )

    def analyze_delta_table(self, table_path: str) -> DataSourceInfo:
        """Analyze Delta table structure and properties."""
        try:
            # Check if it's a Delta table by looking for _delta_log
            if table_path.startswith("s3://"):
                return self._analyze_delta_on_s3(table_path)
            else:
                return self._analyze_delta_local(table_path)

        except Exception as e:
            return DataSourceInfo(
                source_type="delta",
                location=table_path,
                format="delta",
                schema={"error": str(e)},
            )

    def _analyze_delta_on_s3(self, s3_path: str) -> DataSourceInfo:
        """Analyze Delta table on S3."""
        if not self.s3_client:
            return DataSourceInfo(
                source_type="delta",
                location=s3_path,
                format="delta",
                schema={"note": "AWS credentials not available for detailed analysis"},
            )

        # Parse S3 path
        path_parts = s3_path[5:].split("/", 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""

        # Check for _delta_log directory
        delta_log_prefix = f"{prefix}/_delta_log/" if prefix else "_delta_log/"

        response = self.s3_client.list_objects_v2(
            Bucket=bucket, Prefix=delta_log_prefix, MaxKeys=10
        )

        if "Contents" not in response:
            raise ValueError(f"No Delta log found at {s3_path}")

        # Get table size (approximate)
        data_response = self.s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, MaxKeys=1000
        )

        total_size = 0
        if "Contents" in data_response:
            total_size = sum(
                obj["Size"]
                for obj in data_response["Contents"]
                if not obj["Key"].startswith(f"{prefix}/_delta_log/")
            )

        # Detect partitioning from file paths
        partitions = self._detect_partitions(data_response.get("Contents", []))

        return DataSourceInfo(
            source_type="delta",
            location=s3_path,
            format="delta",
            partitions=partitions,
            size_mb=total_size / (1024 * 1024),
            last_modified=max(
                obj["LastModified"] for obj in response["Contents"]
            ).isoformat(),
        )

    def _analyze_delta_local(self, local_path: str) -> DataSourceInfo:
        """Analyze local Delta table."""
        delta_log_path = Path(local_path) / "_delta_log"

        if not delta_log_path.exists():
            raise ValueError(f"No Delta log found at {local_path}")

        # Get directory size
        total_size = sum(
            f.stat().st_size
            for f in Path(local_path).rglob("*")
            if f.is_file() and "_delta_log" not in str(f)
        )

        # Detect partitioning
        data_files = list(Path(local_path).rglob("*.parquet"))
        partitions = self._detect_partitions_from_paths([str(f) for f in data_files])

        return DataSourceInfo(
            source_type="delta",
            location=local_path,
            format="delta",
            partitions=partitions,
            size_mb=total_size / (1024 * 1024),
        )

    def _detect_partitions(self, objects: List[Dict]) -> List[str]:
        """Detect partition columns from S3 object keys."""
        partition_patterns = set()

        for obj in objects:
            key = obj["Key"]
            # Look for partition patterns like year=2023/month=01/
            matches = re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)=([^/]+)", key)
            for match in matches:
                partition_patterns.add(match[0])

        return list(partition_patterns)

    def _detect_partitions_from_paths(self, paths: List[str]) -> List[str]:
        """Detect partition columns from file paths."""
        partition_patterns = set()

        for path in paths:
            # Look for partition patterns in file paths
            matches = re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)=([^/]+)", path)
            for match in matches:
                partition_patterns.add(match[0])

        return list(partition_patterns)

    def _infer_format_from_path(self, path: str) -> str:
        """Infer data format from file path."""
        path_lower = path.lower()

        if ".parquet" in path_lower:
            return "parquet"
        elif ".csv" in path_lower:
            return "csv"
        elif ".json" in path_lower:
            return "json"
        elif ".orc" in path_lower:
            return "orc"
        elif ".avro" in path_lower:
            return "avro"
        elif "_delta_log" in path_lower:
            return "delta"
        else:
            return "unknown"

    def analyze_codebase(self, directory_path: str) -> CodebaseAnalysis:
        """Analyze existing PySpark codebase for patterns and optimization opportunities."""
        directory = Path(directory_path)

        if not directory.exists():
            raise ValueError(f"Directory not found: {directory_path}")

        # Find relevant files
        python_files = list(directory.rglob("*.py"))
        sql_files = list(directory.rglob("*.sql"))

        pyspark_files = []
        common_patterns = []
        data_sources = []
        optimization_opportunities = []
        performance_issues = []
        best_practices_violations = []

        # Analyze Python files for PySpark usage
        for py_file in python_files:
            try:
                content = py_file.read_text(encoding="utf-8")

                # Check if it's a PySpark file
                if self._is_pyspark_file(content):
                    pyspark_files.append(py_file)

                    # Analyze the file
                    file_analysis = self._analyze_pyspark_file(content, str(py_file))

                    common_patterns.extend(file_analysis["patterns"])
                    data_sources.extend(file_analysis["data_sources"])
                    optimization_opportunities.extend(file_analysis["optimizations"])
                    performance_issues.extend(file_analysis["performance_issues"])
                    best_practices_violations.extend(file_analysis["violations"])

            except Exception as e:
                # Skip files that can't be read
                continue

        # Analyze SQL files
        for sql_file in sql_files:
            try:
                content = sql_file.read_text(encoding="utf-8")
                sql_analysis = self._analyze_sql_file(content, str(sql_file))
                optimization_opportunities.extend(sql_analysis["optimizations"])
            except Exception:
                continue

        return CodebaseAnalysis(
            total_files=len(python_files) + len(sql_files),
            pyspark_files=len(pyspark_files),
            sql_files=len(sql_files),
            common_patterns=list(set(common_patterns)),
            data_sources=data_sources,
            optimization_opportunities=list(set(optimization_opportunities)),
            performance_issues=list(set(performance_issues)),
            best_practices_violations=list(set(best_practices_violations)),
        )

    def _is_pyspark_file(self, content: str) -> bool:
        """Check if a Python file uses PySpark."""
        pyspark_indicators = [
            "from pyspark",
            "import pyspark",
            "SparkSession",
            "spark.sql",
            "spark.table",
            "DataFrame",
            ".show()",
            ".collect()",
            ".write.",
        ]

        return any(indicator in content for indicator in pyspark_indicators)

    def _analyze_pyspark_file(self, content: str, file_path: str) -> Dict[str, List]:
        """Analyze a PySpark file for patterns and issues."""
        patterns = []
        data_sources = []
        optimizations = []
        performance_issues = []
        violations = []

        lines = content.split("\n")

        for i, line in enumerate(lines, 1):
            line_stripped = line.strip()

            # Detect data source patterns
            if "spark.read" in line_stripped:
                if ".parquet(" in line_stripped:
                    path_match = re.search(
                        r'\.parquet\(["\']([^"\']+)["\']', line_stripped
                    )
                    if path_match:
                        path = path_match.group(1)
                        data_sources.append(
                            DataSourceInfo(
                                source_type="file", location=path, format="parquet"
                            )
                        )
                elif ".csv(" in line_stripped:
                    path_match = re.search(r'\.csv\(["\']([^"\']+)["\']', line_stripped)
                    if path_match:
                        path = path_match.group(1)
                        data_sources.append(
                            DataSourceInfo(
                                source_type="file", location=path, format="csv"
                            )
                        )

            if "spark.table(" in line_stripped:
                table_match = re.search(
                    r'spark\.table\(["\']([^"\']+)["\']', line_stripped
                )
                if table_match:
                    table = table_match.group(1)
                    data_sources.append(
                        DataSourceInfo(
                            source_type="hive", location=table, format="hive"
                        )
                    )

            # Detect common patterns
            if ".join(" in line_stripped:
                patterns.append("joins")
            if ".groupBy(" in line_stripped:
                patterns.append("aggregations")
            if ".filter(" in line_stripped or ".where(" in line_stripped:
                patterns.append("filtering")
            if ".select(" in line_stripped:
                patterns.append("projections")
            if ".orderBy(" in line_stripped:
                patterns.append("sorting")

            # Detect performance issues
            if ".collect()" in line_stripped:
                performance_issues.append(
                    f"Line {i}: .collect() can cause OOM for large datasets"
                )
            if ".count()" in line_stripped and ".cache()" not in content:
                performance_issues.append(
                    f"Line {i}: .count() without caching may be inefficient"
                )
            if "spark.sql(" in line_stripped and len(line_stripped) > 100:
                performance_issues.append(
                    f"Line {i}: Complex SQL in spark.sql() - consider DataFrame API"
                )

            # Detect best practice violations
            if "spark.createDataFrame(" in line_stripped:
                violations.append(
                    f"Line {i}: Creating DataFrame from local data - consider reading from distributed storage"
                )
            if ".repartition(1)" in line_stripped:
                violations.append(
                    f"Line {i}: repartition(1) creates single partition - use coalesce() for small datasets"
                )
            if "spark.stop()" not in content and "SparkSession.builder" in content:
                violations.append("Missing spark.stop() - may cause resource leaks")

        # Detect optimization opportunities
        if "joins" in patterns and "broadcast" not in content.lower():
            optimizations.append("Consider broadcast joins for small tables")
        if "aggregations" in patterns and "cache" not in content.lower():
            optimizations.append(
                "Consider caching DataFrames used in multiple aggregations"
            )
        if (
            "filtering" in patterns
            and patterns.index("filtering") > patterns.index("projections")
            if "projections" in patterns
            else False
        ):
            optimizations.append(
                "Apply filters before projections for better performance"
            )

        return {
            "patterns": patterns,
            "data_sources": data_sources,
            "optimizations": optimizations,
            "performance_issues": performance_issues,
            "violations": violations,
        }

    def _analyze_sql_file(self, content: str, file_path: str) -> Dict[str, List]:
        """Analyze SQL file for optimization opportunities."""
        optimizations = []

        content_upper = content.upper()

        # Detect optimization opportunities in SQL
        if "SELECT *" in content_upper:
            optimizations.append("Avoid SELECT * - specify only needed columns")
        if "ORDER BY" in content_upper and "LIMIT" not in content_upper:
            optimizations.append(
                "ORDER BY without LIMIT can be expensive - consider if full sorting is needed"
            )
        join_count = content_upper.count("JOIN")
        if join_count > 3:
            optimizations.append(
                "Multiple JOINs detected - consider query optimization and indexing"
            )
        elif join_count > 0:
            optimizations.append(
                "JOIN operations detected - consider optimization strategies"
            )
        if "DISTINCT" in content_upper:
            optimizations.append(
                "DISTINCT operations can be expensive - consider if necessary"
            )

        return {"optimizations": optimizations}

    def generate_optimized_code(
        self, data_source: DataSourceInfo, query_pattern: str
    ) -> str:
        """Generate optimized PySpark code based on data source analysis."""
        code_parts = []

        # Spark session setup
        code_parts.append(
            """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Optimized Spark session configuration
spark = SparkSession.builder \\
    .appName('OptimizedDataProcessing') \\
    .config('spark.sql.adaptive.enabled', 'true') \\
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \\"""
        )

        # Add specific configurations based on data source
        if data_source.size_mb and data_source.size_mb > 1000:  # > 1GB
            code_parts.append(
                """    .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '128MB') \\"""
            )

        if data_source.format == "delta":
            code_parts.append(
                """    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \\"""
            )

        code_parts.append(
            """    .getOrCreate()
"""
        )

        # Data loading based on source type
        if data_source.source_type == "s3":
            if data_source.format == "parquet":
                code_parts.append(
                    f"""
# Load Parquet data from S3
df = spark.read.parquet('{data_source.location}')"""
                )
            elif data_source.format == "delta":
                code_parts.append(
                    f"""
# Load Delta table from S3
df = spark.read.format('delta').load('{data_source.location}')"""
                )
            elif data_source.format == "csv":
                code_parts.append(
                    f"""
# Load CSV data from S3 with optimized settings
df = spark.read \\
    .option('header', 'true') \\
    .option('inferSchema', 'true') \\
    .option('multiline', 'true') \\
    .csv('{data_source.location}')"""
                )

        elif data_source.source_type == "delta":
            code_parts.append(
                f"""
# Load Delta table
df = spark.read.format('delta').load('{data_source.location}')"""
            )

        elif data_source.source_type == "hive":
            code_parts.append(
                f"""
# Load Hive table
df = spark.table('{data_source.location}')"""
            )

        # Add partitioning optimization if partitions detected
        if data_source.partitions:
            code_parts.append(
                f"""
# Partition pruning optimization - filter on partition columns early
# Detected partitions: {', '.join(data_source.partitions)}
# Example: df = df.filter(col('{data_source.partitions[0]}') == 'your_value')"""
            )

        # Add caching recommendation for large datasets
        if data_source.size_mb and data_source.size_mb > 500:  # > 500MB
            code_parts.append(
                """
# Cache DataFrame if it will be used multiple times
# df.cache()"""
            )

        # Add query pattern optimizations
        if "join" in query_pattern.lower():
            code_parts.append(
                """
# Join optimization recommendations:
# 1. Use broadcast joins for small tables (< 200MB)
# 2. Ensure join keys are properly distributed
# Example: df.join(broadcast(small_df), 'key')"""
            )

        if "group" in query_pattern.lower():
            code_parts.append(
                """
# Aggregation optimization recommendations:
# 1. Use appropriate number of shuffle partitions
# 2. Consider pre-aggregation for frequently used metrics
# spark.conf.set('spark.sql.shuffle.partitions', '200')  # Adjust based on data size"""
            )

        code_parts.append(
            """
# Show results with optimized display
df.show(20, truncate=False)

# Optional: Write results with optimization
# df.coalesce(1).write.mode('overwrite').parquet('output_path')
"""
        )

        return "\n".join(code_parts)

    def get_recommendations(
        self,
        data_sources: List[DataSourceInfo],
        codebase_analysis: Optional[CodebaseAnalysis] = None,
    ) -> Dict[str, List[str]]:
        """Get comprehensive recommendations based on data sources and codebase analysis."""
        recommendations = {
            "performance": [],
            "cost_optimization": [],
            "best_practices": [],
            "security": [],
            "monitoring": [],
        }

        # Performance recommendations
        total_size = sum(ds.size_mb or 0 for ds in data_sources)

        if total_size > 10000:  # > 10GB
            recommendations["performance"].append(
                "Consider using Delta Lake for large datasets (>10GB)"
            )
            recommendations["performance"].append(
                "Enable adaptive query execution for better performance"
            )
            recommendations["cost_optimization"].append(
                "Use spot instances for large batch processing jobs"
            )

        # Format-specific recommendations
        formats = [ds.format for ds in data_sources]
        if "csv" in formats:
            recommendations["performance"].append(
                "Convert CSV files to Parquet for better performance"
            )
            recommendations["cost_optimization"].append(
                "Parquet format reduces storage costs by 75-90%"
            )

        if "delta" in formats:
            recommendations["performance"].append(
                "Use Delta Lake time travel for incremental processing"
            )
            recommendations["best_practices"].append(
                "Implement Delta Lake VACUUM for storage optimization"
            )

        # Partitioning recommendations
        partitioned_sources = [ds for ds in data_sources if ds.partitions]
        if partitioned_sources:
            recommendations["performance"].append(
                "Leverage partition pruning in queries"
            )
            recommendations["best_practices"].append(
                "Avoid reading unnecessary partitions"
            )

        # S3 specific recommendations
        s3_sources = [ds for ds in data_sources if ds.source_type == "s3"]
        if s3_sources:
            recommendations["cost_optimization"].append(
                "Use S3 Intelligent Tiering for cost optimization"
            )
            recommendations["security"].append("Enable S3 server-side encryption")
            recommendations["performance"].append(
                "Use S3 Transfer Acceleration for large datasets"
            )

        # Codebase-specific recommendations
        if codebase_analysis:
            if codebase_analysis.performance_issues:
                recommendations["performance"].extend(
                    [
                        "Address identified performance issues in existing code",
                        "Review .collect() usage - consider .take() or .show() instead",
                    ]
                )

            if codebase_analysis.best_practices_violations:
                recommendations["best_practices"].extend(
                    [
                        "Fix best practice violations in existing code",
                        "Implement proper resource management (spark.stop())",
                    ]
                )

        # Monitoring recommendations
        recommendations["monitoring"].extend(
            [
                "Enable Spark History Server for job monitoring",
                "Set up CloudWatch metrics for AWS Glue jobs",
                "Implement data quality checks with Great Expectations",
                "Monitor job costs and performance trends",
            ]
        )

        return recommendations
