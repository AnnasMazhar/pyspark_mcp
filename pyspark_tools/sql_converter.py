"""SQL to PySpark converter using SQLGlot."""

import sqlglot
from typing import Dict, List, Optional, Tuple
import re


class SQLToPySparkConverter:
    """Converts SQL queries to PySpark code with optimizations."""
    
    def __init__(self):
        self.common_optimizations = {
            "broadcast_joins": True,
            "column_pruning": True,
            "predicate_pushdown": True,
            "partition_pruning": True
        }
    
    def convert_sql_to_pyspark(self, sql: str, table_info: Optional[Dict] = None) -> Tuple[str, List[str]]:
        """
        Convert SQL to PySpark code with optimization suggestions.
        
        Args:
            sql: SQL query string
            table_info: Optional table metadata for optimizations
            
        Returns:
            Tuple of (pyspark_code, optimization_suggestions)
        """
        try:
            # Parse SQL using SQLGlot
            parsed = sqlglot.parse_one(sql, dialect="spark")
            
            # Generate base PySpark code
            pyspark_code = self._generate_pyspark_code(parsed, table_info)
            
            # Generate optimization suggestions
            optimizations = self._generate_optimizations(parsed, table_info)
            
            return pyspark_code, optimizations
            
        except Exception as e:
            # Fallback to pattern-based conversion
            return self._fallback_conversion(sql), [f"Warning: Used fallback conversion due to: {str(e)}"]
    
    def _generate_pyspark_code(self, parsed_sql, table_info: Optional[Dict] = None) -> str:
        """Generate PySpark code from parsed SQL."""
        
        # Extract components
        select_clause = self._extract_select_clause(parsed_sql)
        from_clause = self._extract_from_clause(parsed_sql)
        where_clause = self._extract_where_clause(parsed_sql)
        group_by_clause = self._extract_group_by_clause(parsed_sql)
        order_by_clause = self._extract_order_by_clause(parsed_sql)
        
        # Build PySpark code
        code_lines = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "",
            "# Initialize Spark session",
            "spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()",
            ""
        ]
        
        # Add table loading
        if from_clause:
            for table in from_clause:
                code_lines.append(f"# Load table: {table}")
                code_lines.append(f"{table}_df = spark.table('{table}')")
            code_lines.append("")
        
        # Build the main query
        code_lines.append("# Main query")
        query_parts = []
        
        if from_clause:
            main_table = from_clause[0]
            query_parts.append(f"result_df = {main_table}_df")
        
        if where_clause:
            query_parts.append(f"    .filter({where_clause})")
        
        if group_by_clause:
            query_parts.append(f"    .groupBy({group_by_clause})")
        
        if select_clause:
            query_parts.append(f"    .select({select_clause})")
        
        if order_by_clause:
            query_parts.append(f"    .orderBy({order_by_clause})")
        
        code_lines.extend(query_parts)
        code_lines.append("")
        code_lines.append("# Show results")
        code_lines.append("result_df.show()")
        
        return "\n".join(code_lines)
    
    def _extract_select_clause(self, parsed_sql) -> str:
        """Extract SELECT clause and convert to PySpark."""
        try:
            if hasattr(parsed_sql, 'expressions'):
                columns = []
                for expr in parsed_sql.expressions:
                    if hasattr(expr, 'alias') and expr.alias:
                        columns.append(f"col('{expr.this}').alias('{expr.alias}')")
                    else:
                        columns.append(f"col('{expr}')")
                return ", ".join(columns)
        except:
            pass
        return "*"
    
    def _extract_from_clause(self, parsed_sql) -> List[str]:
        """Extract table names from FROM clause."""
        tables = []
        try:
            if hasattr(parsed_sql, 'find_all'):
                for table in parsed_sql.find_all(sqlglot.expressions.Table):
                    tables.append(table.name)
        except:
            pass
        return tables
    
    def _extract_where_clause(self, parsed_sql) -> Optional[str]:
        """Extract WHERE clause and convert to PySpark filter."""
        try:
            if hasattr(parsed_sql, 'find') and parsed_sql.find(sqlglot.expressions.Where):
                where_expr = parsed_sql.find(sqlglot.expressions.Where)
                return self._convert_where_to_filter(str(where_expr.this))
        except:
            pass
        return None
    
    def _extract_group_by_clause(self, parsed_sql) -> Optional[str]:
        """Extract GROUP BY clause."""
        try:
            if hasattr(parsed_sql, 'find') and parsed_sql.find(sqlglot.expressions.Group):
                group_expr = parsed_sql.find(sqlglot.expressions.Group)
                columns = [f"col('{expr}')" for expr in group_expr.expressions]
                return ", ".join(columns)
        except:
            pass
        return None
    
    def _extract_order_by_clause(self, parsed_sql) -> Optional[str]:
        """Extract ORDER BY clause."""
        try:
            if hasattr(parsed_sql, 'find') and parsed_sql.find(sqlglot.expressions.Order):
                order_expr = parsed_sql.find(sqlglot.expressions.Order)
                columns = []
                for expr in order_expr.expressions:
                    if hasattr(expr, 'desc') and expr.desc:
                        columns.append(f"col('{expr.this}').desc()")
                    else:
                        columns.append(f"col('{expr.this}')")
                return ", ".join(columns)
        except:
            pass
        return None
    
    def _convert_where_to_filter(self, where_str: str) -> str:
        """Convert SQL WHERE clause to PySpark filter."""
        # Simple conversions - can be enhanced
        filter_str = where_str
        filter_str = re.sub(r'\b(\w+)\s*=\s*([\'"]?)([^\'"\s]+)\2', r"col('\1') == '\3'", filter_str)
        filter_str = re.sub(r'\b(\w+)\s*>\s*([0-9]+)', r"col('\1') > \2", filter_str)
        filter_str = re.sub(r'\b(\w+)\s*<\s*([0-9]+)', r"col('\1') < \2", filter_str)
        filter_str = re.sub(r'\bAND\b', ' & ', filter_str)
        filter_str = re.sub(r'\bOR\b', ' | ', filter_str)
        return filter_str
    
    def _generate_optimizations(self, parsed_sql, table_info: Optional[Dict] = None) -> List[str]:
        """Generate optimization suggestions."""
        optimizations = []
        
        # Check for joins
        if hasattr(parsed_sql, 'find_all'):
            joins = list(parsed_sql.find_all(sqlglot.expressions.Join))
            if joins:
                optimizations.append("Consider using broadcast joins for small tables")
                optimizations.append("Ensure join keys are properly partitioned")
        
        # Check for aggregations
        if hasattr(parsed_sql, 'find') and parsed_sql.find(sqlglot.expressions.Group):
            optimizations.append("Consider pre-aggregating data if this query runs frequently")
            optimizations.append("Ensure grouping columns are properly partitioned")
        
        # Check for ORDER BY
        if hasattr(parsed_sql, 'find') and parsed_sql.find(sqlglot.expressions.Order):
            optimizations.append("ORDER BY operations are expensive - consider if full sorting is necessary")
        
        # General optimizations
        optimizations.extend([
            "Use column pruning - select only needed columns",
            "Apply filters as early as possible (predicate pushdown)",
            "Consider caching intermediate results if reused",
            "Use appropriate file formats (Parquet, Delta) for better performance"
        ])
        
        return optimizations
    
    def _fallback_conversion(self, sql: str) -> str:
        """Fallback conversion using pattern matching."""
        code_lines = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "",
            "# Initialize Spark session",
            "spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()",
            "",
            "# Note: This is a basic conversion - manual review recommended",
            f"# Original SQL: {sql}",
            "",
            "# Execute SQL directly (consider converting to DataFrame operations)",
            f"result_df = spark.sql('''",
            f"{sql}",
            f"''')",
            "",
            "result_df.show()"
        ]
        
        return "\n".join(code_lines)