"""SQL to PySpark converter using SQLGlot with enhanced dialect support."""

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import sqlglot


@dataclass
class ConversionResult:
    """Result of SQL to PySpark conversion."""

    pyspark_code: str
    optimizations: List[str]
    warnings: List[str]
    dialect_used: str
    complex_constructs: List[str]
    fallback_used: bool


class SQLToPySparkConverter:
    """Converts SQL queries to PySpark code with optimizations."""

    def __init__(self):
        self.common_optimizations = {
            "broadcast_joins": True,
            "column_pruning": True,
            "predicate_pushdown": True,
            "partition_pruning": True,
        }

        # Supported SQL dialects
        self.supported_dialects = {
            "postgres": "postgres",
            "postgresql": "postgres",
            "oracle": "oracle",
            "redshift": "redshift",
            "spark": "spark",
            "hive": "hive",
            "mysql": "mysql",
            "snowflake": "snowflake",
        }

        # Database-specific function mappings
        self.function_mappings = {
            "postgres": {
                "string_agg": "collect_list",
                "array_agg": "collect_list",
                "extract": "date_part",
                "date_trunc": "date_trunc",
                "regexp_replace": "regexp_replace",
                "coalesce": "coalesce",
                "nullif": "nullif",
                "greatest": "greatest",
                "least": "least",
            },
            "oracle": {
                "nvl": "coalesce",
                "nvl2": "when(col('{}').isNotNull(), {}, {})",
                "decode": "when",
                "rownum": "row_number()",
                "sysdate": "current_timestamp()",
                "trunc": "trunc",
                "to_char": "date_format",
                "to_date": "to_date",
                "substr": "substring",
            },
            "redshift": {
                "dateadd": "date_add",
                "datediff": "datediff",
                "getdate": "current_timestamp()",
                "isnull": "coalesce",
                "len": "length",
                "charindex": "instr",
                "stuff": "overlay",
                "convert": "cast",
            },
        }

        self.logger = logging.getLogger(__name__)

    def convert_sql_to_pyspark(
        self, sql: str, table_info: Optional[Dict] = None, dialect: Optional[str] = None
    ) -> ConversionResult:
        """
        Convert SQL to PySpark code with enhanced dialect support.

        Args:
            sql: SQL query string
            table_info: Optional table metadata for optimizations
            dialect: Source SQL dialect (postgres, oracle, redshift, etc.)

        Returns:
            ConversionResult with code, optimizations, and metadata
        """
        warnings = []
        complex_constructs = []
        fallback_used = False

        # Auto-detect dialect if not provided
        if not dialect:
            dialect = self._detect_dialect(sql)

        # Normalize dialect name
        dialect = self.supported_dialects.get(dialect.lower(), "spark")

        try:
            # Parse SQL using detected/specified dialect
            parsed = sqlglot.parse_one(sql, dialect=dialect)

            # Check if fallback is required for unsupported features
            if self._requires_fallback(sql, parsed):
                fallback_code, fallback_guidance = self._enhanced_fallback_conversion(
                    sql, dialect
                )
                warnings.extend(fallback_guidance)

                return ConversionResult(
                    pyspark_code=fallback_code,
                    optimizations=[],
                    warnings=warnings,
                    dialect_used=dialect,
                    complex_constructs=["Recursive CTE (unsupported)"],
                    fallback_used=True,
                )

            # Analyze complex constructs
            complex_constructs = self._analyze_complex_constructs(parsed)

            # Generate enhanced PySpark code
            pyspark_code = self._generate_enhanced_pyspark_code(
                parsed, table_info, dialect
            )

            # Generate dialect-specific optimizations
            optimizations = self._generate_dialect_optimizations(
                parsed, table_info, dialect
            )

            return ConversionResult(
                pyspark_code=pyspark_code,
                optimizations=optimizations,
                warnings=warnings,
                dialect_used=dialect,
                complex_constructs=complex_constructs,
                fallback_used=fallback_used,
            )

        except Exception as e:
            self.logger.warning(f"SQL parsing failed for dialect {dialect}: {str(e)}")

            # Try fallback with different dialects
            for fallback_dialect in ["spark", "postgres", "mysql"]:
                if fallback_dialect != dialect:
                    try:
                        parsed = sqlglot.parse_one(sql, dialect=fallback_dialect)
                        warnings.append(
                            f"Parsed using {fallback_dialect} dialect instead of {dialect}"
                        )

                        complex_constructs = self._analyze_complex_constructs(parsed)
                        pyspark_code = self._generate_enhanced_pyspark_code(
                            parsed, table_info, fallback_dialect
                        )
                        optimizations = self._generate_dialect_optimizations(
                            parsed, table_info, fallback_dialect
                        )

                        return ConversionResult(
                            pyspark_code=pyspark_code,
                            optimizations=optimizations,
                            warnings=warnings,
                            dialect_used=fallback_dialect,
                            complex_constructs=complex_constructs,
                            fallback_used=True,
                        )
                    except:
                        continue

            # Final fallback to pattern-based conversion
            fallback_code, fallback_guidance = self._enhanced_fallback_conversion(
                sql, dialect
            )
            warnings.extend(fallback_guidance)

            return ConversionResult(
                pyspark_code=fallback_code,
                optimizations=[],
                warnings=warnings,
                dialect_used=dialect,
                complex_constructs=[],
                fallback_used=True,
            )

    def _detect_dialect(self, sql: str) -> str:
        """Auto-detect SQL dialect based on syntax patterns."""
        sql_lower = sql.lower()

        # PostgreSQL indicators
        if any(
            keyword in sql_lower
            for keyword in ["string_agg", "array_agg", "extract(", "regexp_replace"]
        ):
            return "postgres"

        # Oracle indicators
        if any(
            keyword in sql_lower
            for keyword in ["nvl(", "decode(", "rownum", "sysdate", "dual"]
        ):
            return "oracle"

        # Redshift indicators
        if any(
            keyword in sql_lower
            for keyword in ["dateadd(", "datediff(", "getdate()", "isnull("]
        ):
            return "redshift"

        # Default to Spark/Hive
        return "spark"

    def _analyze_complex_constructs(self, parsed_sql) -> List[str]:
        """Analyze SQL for complex constructs that need special handling."""
        constructs = []

        try:
            # Check for CTEs (Common Table Expressions)
            if hasattr(parsed_sql, "find_all"):
                ctes = list(parsed_sql.find_all(sqlglot.expressions.CTE))
                if ctes:
                    constructs.append(f"CTEs ({len(ctes)} found)")

                # Check for window functions
                window_funcs = list(parsed_sql.find_all(sqlglot.expressions.Window))
                if window_funcs:
                    constructs.append(f"Window functions ({len(window_funcs)} found)")

                # Check for subqueries
                subqueries = list(parsed_sql.find_all(sqlglot.expressions.Subquery))
                if subqueries:
                    constructs.append(f"Subqueries ({len(subqueries)} found)")

                # Check for complex joins
                joins = list(parsed_sql.find_all(sqlglot.expressions.Join))
                if len(joins) > 2:
                    constructs.append(f"Complex joins ({len(joins)} joins)")

                # Check for CASE statements
                case_stmts = list(parsed_sql.find_all(sqlglot.expressions.Case))
                if case_stmts:
                    constructs.append(f"CASE statements ({len(case_stmts)} found)")

                # Check for aggregate functions
                agg_funcs = list(parsed_sql.find_all(sqlglot.expressions.AggFunc))
                if len(agg_funcs) > 3:
                    constructs.append(
                        f"Multiple aggregations ({len(agg_funcs)} functions)"
                    )

        except Exception as e:
            self.logger.warning(f"Error analyzing complex constructs: {str(e)}")

        return constructs

    def _requires_fallback(self, sql: str, parsed_sql) -> bool:
        """Check if SQL requires fallback conversion due to unsupported features."""
        sql_lower = sql.lower()

        # Check for recursive CTEs (not supported in PySpark)
        if "with recursive" in sql_lower:
            return True

        # Check for other unsupported features
        unsupported_patterns = [
            "pivot",
            "unpivot",
            "merge",
            "connect by",
            "start with",
        ]

        return any(pattern in sql_lower for pattern in unsupported_patterns)

    def _generate_enhanced_pyspark_code(
        self, parsed_sql, table_info: Optional[Dict] = None, dialect: str = "spark"
    ) -> str:
        """Generate enhanced PySpark code from parsed SQL with dialect support."""

        code_lines = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "from pyspark.sql.window import Window",
            "",
            f"# Generated from {dialect.upper()} SQL",
            "spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()",
            "",
        ]

        # Handle CTEs first
        cte_code = self._handle_ctes(parsed_sql, dialect)
        if cte_code:
            code_lines.extend(cte_code)
            code_lines.append("")

        # Extract and handle main query components
        from_clause = self._extract_from_clause(parsed_sql)

        # Add table loading - extract all unique table names
        all_tables = self._extract_all_tables(parsed_sql)
        if all_tables:
            for table_name in all_tables:
                code_lines.append(f"# Load table: {table_name}")
                code_lines.append(f"{table_name}_df = spark.table('{table_name}')")
            code_lines.append("")

        # Handle subqueries
        subquery_code = self._handle_subqueries(parsed_sql, dialect)
        if subquery_code:
            code_lines.extend(subquery_code)
            code_lines.append("")

        # Build main query with enhanced features
        main_query_code = self._build_main_query(parsed_sql, dialect)
        code_lines.extend(main_query_code)

        code_lines.append("")
        code_lines.append("# Show results")
        code_lines.append("result_df.show()")

        return "\n".join(code_lines)

    def _handle_ctes(self, parsed_sql, dialect: str) -> List[str]:
        """Handle Common Table Expressions (CTEs)."""
        code_lines = []

        try:
            if hasattr(parsed_sql, "find_all"):
                ctes = list(parsed_sql.find_all(sqlglot.expressions.CTE))

                if ctes:
                    code_lines.append("# Handle CTEs (Common Table Expressions)")

                    for i, cte in enumerate(ctes):
                        cte_name = cte.alias if hasattr(cte, "alias") else f"cte_{i}"
                        code_lines.append(f"# CTE: {cte_name}")

                        # For complex CTEs, we'll create temporary views
                        code_lines.append(f"{cte_name}_df = spark.sql('''")
                        code_lines.append(f"    {str(cte.this)}")
                        code_lines.append("''')")
                        code_lines.append(
                            f"{cte_name}_df.createOrReplaceTempView('{cte_name}')"
                        )
                        code_lines.append("")

        except Exception as e:
            self.logger.warning(f"Error handling CTEs: {str(e)}")
            code_lines.append(
                "# Note: CTE handling encountered issues - manual review recommended"
            )

        return code_lines

    def _handle_subqueries(self, parsed_sql, dialect: str) -> List[str]:
        """Handle complex subqueries."""
        code_lines = []

        try:
            if hasattr(parsed_sql, "find_all"):
                subqueries = list(parsed_sql.find_all(sqlglot.expressions.Subquery))

                if subqueries:
                    code_lines.append("# Handle subqueries")

                    for i, subquery in enumerate(subqueries):
                        subquery_name = f"subquery_{i}"
                        code_lines.append(f"# Subquery {i+1}")
                        code_lines.append(f"{subquery_name}_df = spark.sql('''")
                        code_lines.append(f"    {str(subquery.this)}")
                        code_lines.append("''')")
                        code_lines.append(
                            f"{subquery_name}_df.createOrReplaceTempView('{subquery_name}')"
                        )
                        code_lines.append("")

        except Exception as e:
            self.logger.warning(f"Error handling subqueries: {str(e)}")
            code_lines.append(
                "# Note: Subquery handling encountered issues - manual review recommended"
            )

        return code_lines

    def _build_main_query(self, parsed_sql, dialect: str) -> List[str]:
        """Build the main query with enhanced features."""
        code_lines = ["# Main query"]

        try:
            # Extract components with enhanced handling
            select_clause = self._extract_enhanced_select_clause(parsed_sql, dialect)
            from_clause = self._extract_from_clause(parsed_sql)
            joins = self._extract_joins(parsed_sql, dialect)
            where_clause = self._extract_where_clause(parsed_sql)
            group_by_clause = self._extract_group_by_clause(parsed_sql)
            having_clause = self._extract_having_clause(parsed_sql)
            window_functions = self._extract_window_functions(parsed_sql, dialect)
            order_by_clause = self._extract_order_by_clause(parsed_sql)

            # Build query chain
            query_parts = []

            if from_clause:
                main_table_name, main_table_alias = from_clause[0]
                query_parts.append(
                    f"result_df = {main_table_name}_df.alias('{main_table_alias}')"
                )

            # Add joins
            if joins:
                query_parts.extend(joins)

            # Add filters
            if where_clause:
                query_parts.append(f"    .filter({where_clause})")

            # Add window functions if needed
            if window_functions:
                query_parts.extend(window_functions)

            # Add grouping
            if group_by_clause:
                query_parts.append(f"    .groupBy({group_by_clause})")

            # Add having clause
            if having_clause:
                query_parts.append(f"    .filter({having_clause})  # HAVING clause")

            # Add selection
            if select_clause:
                query_parts.append(f"    .select({select_clause})")

            # Add ordering
            if order_by_clause:
                query_parts.append(f"    .orderBy({order_by_clause})")

            code_lines.extend(query_parts)

        except Exception as e:
            self.logger.warning(f"Error building main query: {str(e)}")
            code_lines.append(
                "# Note: Query building encountered issues - using SQL fallback"
            )
            code_lines.append("result_df = spark.sql('''")
            code_lines.append(f"    {str(parsed_sql)}")
            code_lines.append("''')")

        return code_lines

    def _extract_enhanced_select_clause(self, parsed_sql, dialect: str) -> str:
        """Extract SELECT clause with enhanced dialect-specific function handling."""
        try:
            if hasattr(parsed_sql, "expressions"):
                # Build alias map for resolving references within same SELECT
                alias_map = {}
                for expr in parsed_sql.expressions:
                    if hasattr(expr, "alias") and expr.alias:
                        alias_map[expr.alias] = expr.this

                # Convert expressions, resolving alias references
                columns = []
                for expr in parsed_sql.expressions:
                    column_expr = self._convert_expression_to_pyspark_with_aliases(
                        expr, dialect, alias_map
                    )
                    columns.append(column_expr)
                return ", ".join(columns)
        except Exception as e:
            self.logger.warning(f"Error extracting select clause: {str(e)}")
        return "*"

    def _convert_expression_to_pyspark_with_aliases(
        self, expr, dialect: str, alias_map: dict
    ) -> str:
        """Convert expression, resolving alias references from the same SELECT clause."""
        # Check if this is a column reference to an alias in the same SELECT
        if isinstance(expr, sqlglot.expressions.Alias) and hasattr(expr, "this"):
            resolved_expr = self._resolve_alias_references(expr.this, alias_map)
            converted = self._convert_expression_to_pyspark(resolved_expr, dialect)
            alias = str(expr.alias).strip('"').strip("'")
            return f"({converted}).alias('{alias}')"

        resolved_expr = self._resolve_alias_references(expr, alias_map)
        return self._convert_expression_to_pyspark(resolved_expr, dialect)

    def _resolve_alias_references(self, expr, alias_map: dict):
        """Recursively resolve column references that point to aliases in the same SELECT."""
        if isinstance(expr, sqlglot.expressions.Column):
            col_name = expr.name if hasattr(expr, "name") else str(expr)
            # If this column name matches an alias, replace with the actual expression
            if col_name in alias_map:
                return alias_map[col_name]

        # Recursively resolve in child expressions
        if hasattr(expr, "args"):
            for key, value in expr.args.items():
                if isinstance(value, sqlglot.expressions.Expression):
                    expr.args[key] = self._resolve_alias_references(value, alias_map)
                elif isinstance(value, list):
                    expr.args[key] = [
                        (
                            self._resolve_alias_references(v, alias_map)
                            if isinstance(v, sqlglot.expressions.Expression)
                            else v
                        )
                        for v in value
                    ]

        return expr
        return "*"

    def _convert_expression_to_pyspark(self, expr, dialect: str) -> str:
        """Convert SQL expression to PySpark with dialect-specific function mapping."""
        try:
            # Handle aliases
            if hasattr(expr, "alias") and expr.alias:
                base_expr = self._convert_expression_to_pyspark(expr.this, dialect)
                alias = str(expr.alias).strip('"').strip("'")
                return f"({base_expr}).alias('{alias}')"

            # Handle Column references
            if isinstance(expr, sqlglot.expressions.Column):
                table = expr.table if hasattr(expr, "table") and expr.table else None
                column = expr.name if hasattr(expr, "name") else str(expr)
                column = column.strip('"').strip("'")

                if table:
                    return f"col('{table}.{column}')"
                else:
                    return f"col('{column}')"

            # Handle aggregate functions
            if isinstance(expr, sqlglot.expressions.Count):
                if hasattr(expr, "this") and expr.this:
                    # Check if this is COUNT(DISTINCT ...)
                    if isinstance(expr.this, sqlglot.expressions.Distinct):
                        if expr.this.expressions:
                            arg = self._convert_expression_to_pyspark(
                                expr.this.expressions[0], dialect
                            )
                            return f"countDistinct({arg})"
                    arg = self._convert_expression_to_pyspark(expr.this, dialect)
                    return f"count({arg})"
                return "count('*')"

            # Handle other common functions
            expr_type = type(expr).__name__

            if expr_type in ["Sum", "Avg", "Min", "Max"]:
                func_name = expr_type.lower()
                arg = self._convert_expression_to_pyspark(expr.this, dialect)
                return f"{func_name}({arg})"

            # Handle ROUND
            if isinstance(expr, sqlglot.expressions.Round):
                arg = self._convert_expression_to_pyspark(expr.this, dialect)
                decimals = expr.args.get("decimals")
                if decimals:
                    dec_val = self._convert_expression_to_pyspark(decimals, dialect)
                    return f"round({arg}, {dec_val})"
                return f"round({arg})"

            # Handle NULLIF
            if isinstance(expr, sqlglot.expressions.Nullif):
                arg1 = self._convert_expression_to_pyspark(expr.this, dialect)
                arg2 = self._convert_expression_to_pyspark(expr.expression, dialect)
                return f"when({arg1} == {arg2}, lit(None)).otherwise({arg1})"

            # Handle CASE expressions
            if isinstance(expr, sqlglot.expressions.Case):
                conditions = []
                for i, when_expr in enumerate(expr.args.get("ifs", [])):
                    condition = self._convert_expression_to_filter(when_expr.this)
                    value = self._convert_expression_to_pyspark(
                        when_expr.args.get("true"), dialect
                    )
                    if i == 0:
                        conditions.append(f"when({condition}, {value})")
                    else:
                        conditions.append(f".when({condition}, {value})")

                # Handle ELSE clause
                else_value = expr.args.get("default")
                if else_value:
                    else_val = self._convert_expression_to_pyspark(else_value, dialect)
                    conditions.append(f".otherwise({else_val})")

                return "".join(conditions)

            # Handle arithmetic operations
            if isinstance(
                expr,
                (
                    sqlglot.expressions.Div,
                    sqlglot.expressions.Mul,
                    sqlglot.expressions.Add,
                    sqlglot.expressions.Sub,
                ),
            ):
                left = self._convert_expression_to_pyspark(expr.this, dialect)
                right = self._convert_expression_to_pyspark(expr.expression, dialect)
                op = (
                    "/"
                    if isinstance(expr, sqlglot.expressions.Div)
                    else (
                        "*"
                        if isinstance(expr, sqlglot.expressions.Mul)
                        else "+" if isinstance(expr, sqlglot.expressions.Add) else "-"
                    )
                )
                return f"({left} {op} {right})"

            # Handle CAST
            if isinstance(expr, sqlglot.expressions.Cast):
                arg = self._convert_expression_to_pyspark(expr.this, dialect)
                to_type = str(expr.to).lower()
                # Map SQL types to PySpark types
                if "decimal" in to_type or "numeric" in to_type:
                    return f"{arg}.cast('decimal(10,2)')"
                elif "int" in to_type:
                    return f"{arg}.cast('int')"
                elif "string" in to_type or "varchar" in to_type:
                    return f"{arg}.cast('string')"
                return f"{arg}.cast('{to_type}')"

            # Handle literals
            if isinstance(expr, sqlglot.expressions.Literal):
                if expr.is_string:
                    return f"lit('{expr.this}')"
                return f"lit({expr.this})"

            # Fallback to string representation
            expr_str = str(expr).strip('"').strip("'")
            return f"col('{expr_str}')"

        except Exception as e:
            self.logger.warning(f"Error converting expression: {str(e)}")
            return f"col('{str(expr)}')".replace('"', "")

    def _extract_joins(self, parsed_sql, dialect: str) -> List[str]:
        self.logger.warning(f"Error converting expression {expr}: {str(e)}")
        return f"col('{str(expr)}')"

    def _convert_function_calls(self, expr_str: str, dialect: str) -> str:
        """Convert dialect-specific function calls to PySpark equivalents."""
        if dialect not in self.function_mappings:
            return f"col('{expr_str}')"

        mappings = self.function_mappings[dialect]

        for sql_func, pyspark_func in mappings.items():
            # Simple function replacement
            if sql_func in expr_str.lower():
                if "{}" in pyspark_func:
                    # Complex mapping requiring parameter substitution
                    continue  # Handle in _map_dialect_function
                else:
                    expr_str = re.sub(
                        rf"\b{sql_func}\b", pyspark_func, expr_str, flags=re.IGNORECASE
                    )

        return (
            f"expr('{expr_str}')" if expr_str != str(expr_str) else f"col('{expr_str}')"
        )

    def _map_dialect_function(self, func_name: str, args: List, dialect: str) -> str:
        """Map dialect-specific functions with complex parameter handling."""
        if dialect not in self.function_mappings:
            return f"expr('{func_name}({', '.join(str(arg) for arg in args)})')"

        mappings = self.function_mappings[dialect]

        if func_name in mappings:
            pyspark_func = mappings[func_name]

            # Handle special cases
            if func_name == "nvl2" and len(args) >= 3:
                return f"when(col('{args[0]}').isNotNull(), {args[1]}, {args[2]})"
            elif func_name == "decode" and len(args) >= 3:
                # Convert Oracle DECODE to CASE WHEN
                decode_expr = f"when(col('{args[0]}') == '{args[1]}', {args[2]})"
                for i in range(3, len(args), 2):
                    if i + 1 < len(args):
                        decode_expr += (
                            f".when(col('{args[0]}') == '{args[i]}', {args[i+1]})"
                        )
                if len(args) % 2 == 0:  # Has default value
                    decode_expr += f".otherwise({args[-1]})"
                return decode_expr
            else:
                # Simple function mapping
                args_str = ", ".join(f"col('{arg}')" for arg in args)
                return f"{pyspark_func}({args_str})"

        return f"expr('{func_name}({', '.join(str(arg) for arg in args)})')"

    def _extract_joins(self, parsed_sql, dialect: str) -> List[str]:
        """Extract and convert JOIN clauses."""
        join_lines = []

        try:
            if hasattr(parsed_sql, "find_all"):
                joins = list(parsed_sql.find_all(sqlglot.expressions.Join))

                for join in joins:
                    # Extract join type
                    join_type = "inner"
                    if hasattr(join, "side") and join.side:
                        join_type = str(join.side).lower()
                    elif hasattr(join, "kind") and join.kind:
                        join_type = str(join.kind).lower()

                    if hasattr(join, "this"):
                        # Extract table name and alias
                        right_table_expr = join.this
                        table_name = None
                        table_alias = None

                        if hasattr(right_table_expr, "name"):
                            table_name = right_table_expr.name
                        if (
                            hasattr(right_table_expr, "alias")
                            and right_table_expr.alias
                        ):
                            table_alias = right_table_expr.alias

                        # Use table name for dataframe variable, alias for column references
                        df_var = table_name if table_name else table_alias
                        alias_name = table_alias if table_alias else table_name

                        # Get join condition from args
                        join_condition = "True"
                        if hasattr(join, "args") and "on" in join.args:
                            join_condition = self._convert_join_condition(
                                join.args["on"]
                            )

                        join_lines.append(
                            f"    .join({df_var}_df.alias('{alias_name}'), {join_condition}, '{join_type}')"
                        )

        except Exception as e:
            self.logger.warning(f"Error extracting joins: {str(e)}")

        return join_lines

    def _convert_join_condition(self, condition_expr) -> str:
        """Convert SQL join condition to PySpark expression."""
        try:
            # Handle AND conditions (multiple join keys)
            if isinstance(condition_expr, sqlglot.expressions.And):
                left_cond = self._convert_join_condition(condition_expr.left)
                right_cond = self._convert_join_condition(condition_expr.right)
                return f"({left_cond}) & ({right_cond})"

            # Handle EQ (equality) conditions
            if isinstance(condition_expr, sqlglot.expressions.EQ):
                left = self._convert_column_ref(condition_expr.left)
                right = self._convert_column_ref(condition_expr.right)
                return f"({left} == {right})"

            # Fallback
            return "True"
        except Exception as e:
            self.logger.warning(f"Error converting join condition: {str(e)}")
            return "True"

    def _convert_column_ref(self, col_expr) -> str:
        """Convert column reference to PySpark col() expression."""
        try:
            if isinstance(col_expr, sqlglot.expressions.Column):
                table_alias = (
                    col_expr.table
                    if hasattr(col_expr, "table") and col_expr.table
                    else None
                )
                column = col_expr.name if hasattr(col_expr, "name") else str(col_expr)

                # Remove quotes from column name
                column = column.strip('"').strip("'")

                if table_alias:
                    # For now, just use col() with table.column notation
                    # This works when dataframes are properly aliased in joins
                    return f"col('{table_alias}.{column}')"
                else:
                    return f"col('{column}')"
            elif isinstance(col_expr, sqlglot.expressions.Literal):
                # Handle literals in join conditions
                value = col_expr.this
                if col_expr.is_string:
                    return f"lit('{value}')"
                else:
                    return f"lit({value})"
            elif isinstance(col_expr, sqlglot.expressions.Cast):
                # Handle CAST expressions in join conditions
                inner = self._convert_column_ref(col_expr.this)
                to_type = str(col_expr.to).lower()
                # Map SQL types to PySpark types
                if "int" in to_type:
                    return f"{inner}.cast('int')"
                elif "decimal" in to_type or "numeric" in to_type:
                    return f"{inner}.cast('decimal(10,2)')"
                elif "string" in to_type or "varchar" in to_type:
                    return f"{inner}.cast('string')"
                return f"{inner}.cast('{to_type}')"
            else:
                col_str = str(col_expr).strip('"').strip("'")
                return f"col('{col_str}')"
        except Exception:
            return f"col('{str(col_expr)}')"

    def _extract_having_clause(self, parsed_sql) -> Optional[str]:
        """Extract HAVING clause."""
        try:
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Having
            ):
                having_expr = parsed_sql.find(sqlglot.expressions.Having)
                return self._convert_expression_to_filter(having_expr.this)
        except Exception as e:
            self.logger.warning(f"Error extracting having clause: {str(e)}")
        return None

    def _extract_window_functions(self, parsed_sql, dialect: str) -> List[str]:
        """Extract and convert window functions."""
        window_lines = []

        try:
            if hasattr(parsed_sql, "find_all"):
                windows = list(parsed_sql.find_all(sqlglot.expressions.Window))

                if windows:
                    window_lines.append("    # Window functions")

                    for i, window in enumerate(windows):
                        window_spec = self._build_window_spec(window, dialect)
                        window_lines.append(
                            f"    .withColumn('window_col_{i}', {window_spec})"
                        )

        except Exception as e:
            self.logger.warning(f"Error extracting window functions: {str(e)}")

        return window_lines

    def _build_window_spec(self, window_expr, dialect: str) -> str:
        """Build window specification for PySpark."""
        try:
            # Extract window components
            partition_by = []
            order_by = []

            if hasattr(window_expr, "partition_by") and window_expr.partition_by:
                partition_by = [f"col('{col}')" for col in window_expr.partition_by]

            if hasattr(window_expr, "order") and window_expr.order:
                order_by = [f"col('{col}')" for col in window_expr.order]

            # Build window spec
            window_spec = "Window"
            if partition_by:
                window_spec += f".partitionBy({', '.join(partition_by)})"
            if order_by:
                window_spec += f".orderBy({', '.join(order_by)})"

            # Get the window function
            func_name = "row_number"  # default
            if hasattr(window_expr, "this"):
                func_name = str(window_expr.this).lower()

            return f"{func_name}().over({window_spec})"

        except Exception as e:
            self.logger.warning(f"Error building window spec: {str(e)}")
            return "row_number().over(Window.partitionBy())"

    def _extract_from_clause(self, parsed_sql) -> List[tuple]:
        """Extract table names and aliases from FROM clause."""
        tables = []
        try:
            if hasattr(parsed_sql, "find"):
                # Get the main FROM table (not joins)
                from_expr = parsed_sql.find(sqlglot.expressions.From)
                if from_expr and hasattr(from_expr, "this"):
                    table = from_expr.this
                    if hasattr(table, "name"):
                        table_name = table.name
                        table_alias = (
                            table.alias
                            if hasattr(table, "alias") and table.alias
                            else table_name
                        )
                        tables.append((table_name, table_alias))
        except:
            pass
        return tables

    def _extract_all_tables(self, parsed_sql) -> List[str]:
        """Extract all unique table names for loading."""
        tables = set()
        try:
            if hasattr(parsed_sql, "find_all"):
                for table in parsed_sql.find_all(sqlglot.expressions.Table):
                    if hasattr(table, "name"):
                        tables.add(table.name)
        except:
            pass
        return sorted(list(tables))

    def _extract_where_clause(self, parsed_sql) -> Optional[str]:
        """Extract WHERE clause and convert to PySpark filter."""
        try:
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Where
            ):
                where_expr = parsed_sql.find(sqlglot.expressions.Where)
                return self._convert_expression_to_filter(where_expr.this)
        except:
            pass
        return None

    def _convert_expression_to_filter(self, expr) -> str:
        """Convert SQL expression to PySpark filter expression."""
        try:
            # Handle Paren (parentheses wrapper)
            if isinstance(expr, sqlglot.expressions.Paren):
                return self._convert_expression_to_filter(expr.this)

            # Handle AND
            if isinstance(expr, sqlglot.expressions.And):
                left = self._convert_expression_to_filter(expr.left)
                right = self._convert_expression_to_filter(expr.right)
                return f"({left}) & ({right})"

            # Handle OR
            if isinstance(expr, sqlglot.expressions.Or):
                left = self._convert_expression_to_filter(expr.left)
                right = self._convert_expression_to_filter(expr.right)
                return f"({left}) | ({right})"

            # Handle comparisons
            if isinstance(expr, sqlglot.expressions.EQ):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} == {right})"

            if isinstance(expr, sqlglot.expressions.NEQ):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} != {right})"

            if isinstance(expr, sqlglot.expressions.GT):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} > {right})"

            if isinstance(expr, sqlglot.expressions.GTE):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} >= {right})"

            if isinstance(expr, sqlglot.expressions.LT):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} < {right})"

            if isinstance(expr, sqlglot.expressions.LTE):
                left = self._convert_filter_operand(expr.left)
                right = self._convert_filter_operand(expr.right)
                return f"({left} <= {right})"

            # Handle LIKE
            if isinstance(expr, sqlglot.expressions.Like):
                left = self._convert_filter_operand(expr.this)
                pattern = self._convert_filter_operand(expr.expression)
                return f"{left}.like({pattern})"

            # Handle IN
            if isinstance(expr, sqlglot.expressions.In):
                left = self._convert_filter_operand(expr.this)
                values = [self._convert_filter_operand(v) for v in expr.expressions]
                return f"{left}.isin([{', '.join(values)}])"

            # Handle IS NULL / IS NOT NULL
            if isinstance(expr, sqlglot.expressions.Is):
                left = self._convert_filter_operand(expr.this)
                # Check if comparing to NULL
                if isinstance(expr.expression, sqlglot.expressions.Null):
                    return f"{left}.isNull()"
                return f"({left} == {self._convert_filter_operand(expr.expression)})"

            # Handle NOT
            if isinstance(expr, sqlglot.expressions.Not):
                inner = self._convert_expression_to_filter(expr.this)
                # Special case: IS NULL becomes isNotNull()
                if isinstance(expr.this, sqlglot.expressions.Is):
                    left = self._convert_filter_operand(expr.this.this)
                    if isinstance(expr.this.expression, sqlglot.expressions.Null):
                        return f"{left}.isNotNull()"
                return f"(~{inner})"

            # Fallback
            return f"col('{str(expr)}')"
        except Exception as e:
            self.logger.warning(f"Error converting filter expression: {str(e)}")
            return f"col('{str(expr)}')"

    def _convert_filter_operand(self, operand) -> str:
        """Convert filter operand (column or literal)."""
        try:
            # Handle column references
            if isinstance(operand, sqlglot.expressions.Column):
                table = (
                    operand.table
                    if hasattr(operand, "table") and operand.table
                    else None
                )
                column = operand.name if hasattr(operand, "name") else str(operand)
                column = column.strip('"').strip("'")

                if table:
                    return f"col('{table}.{column}')"
                else:
                    return f"col('{column}')"

            # Handle literals
            if isinstance(operand, sqlglot.expressions.Literal):
                value = operand.this
                if operand.is_string:
                    return f"lit('{value}')"
                else:
                    return f"lit({value})"

            # Handle date arithmetic (NOW() - INTERVAL)
            if isinstance(operand, sqlglot.expressions.Sub):
                left_expr = operand.this
                right = operand.expression

                # Check if right side is an INTERVAL
                if isinstance(right, sqlglot.expressions.Interval):
                    interval_value = str(right.this).strip("'")
                    interval_unit = (
                        str(right.unit).upper() if hasattr(right, "unit") else "DAYS"
                    )

                    # Convert to PySpark date functions without using expr()
                    if interval_unit in ["HOUR", "HOURS"]:
                        return f"date_sub(current_timestamp(), {interval_value}/24)"
                    elif interval_unit in ["DAY", "DAYS"]:
                        return f"date_sub(current_date(), {interval_value})"
                    else:
                        # Fallback
                        return f"current_timestamp()"

                # Regular subtraction
                left_val = self._convert_filter_operand(left_expr)
                right_val = self._convert_filter_operand(right)
                return f"({left_val} - {right_val})"

            # Handle Anonymous functions (like NOW())
            if isinstance(operand, sqlglot.expressions.Anonymous):
                func_name = str(operand.this).upper()
                if func_name in ["NOW", "CURRENT_TIMESTAMP"]:
                    return "current_timestamp()"
                elif func_name == "CURRENT_DATE":
                    return "current_date()"

            # Fallback
            val_str = str(operand).strip('"').strip("'")
            # Don't quote if it's a function call (contains parentheses)
            if "(" in val_str and ")" in val_str:
                return val_str
            if not val_str.replace(".", "").replace("-", "").isdigit():
                return f"'{val_str}'"
            return val_str
        except Exception:
            return f"'{str(operand)}'"

    def _extract_group_by_clause(self, parsed_sql) -> Optional[str]:
        """Extract GROUP BY clause."""
        try:
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Group
            ):
                group_expr = parsed_sql.find(sqlglot.expressions.Group)
                columns = []

                # Get SELECT expressions for resolving numeric references
                select_exprs = []
                if hasattr(parsed_sql, "find") and parsed_sql.find(
                    sqlglot.expressions.Select
                ):
                    select_node = parsed_sql.find(sqlglot.expressions.Select)
                    select_exprs = (
                        select_node.expressions
                        if hasattr(select_node, "expressions")
                        else []
                    )

                for expr in group_expr.expressions:
                    # Handle numeric references (GROUP BY 1, 2, etc.)
                    if (
                        isinstance(expr, sqlglot.expressions.Literal)
                        and not expr.is_string
                    ):
                        try:
                            idx = int(str(expr.this)) - 1  # SQL uses 1-based indexing
                            if 0 <= idx < len(select_exprs):
                                # Get the column from SELECT
                                select_expr = select_exprs[idx]
                                if isinstance(select_expr, sqlglot.expressions.Alias):
                                    col_name = select_expr.alias
                                elif isinstance(
                                    select_expr, sqlglot.expressions.Column
                                ):
                                    col_name = (
                                        select_expr.name
                                        if hasattr(select_expr, "name")
                                        else str(select_expr)
                                    )
                                else:
                                    col_name = str(select_expr)
                                col_name = col_name.strip('"').strip("'")
                                columns.append(f"col('{col_name}')")
                                continue
                        except:
                            pass

                    if isinstance(expr, sqlglot.expressions.Column):
                        table = (
                            expr.table
                            if hasattr(expr, "table") and expr.table
                            else None
                        )
                        column = expr.name if hasattr(expr, "name") else str(expr)
                        column = column.strip('"').strip("'")

                        if table:
                            columns.append(f"col('{table}.{column}')")
                        else:
                            columns.append(f"col('{column}')")
                    else:
                        col_str = str(expr).strip('"').strip("'")
                        columns.append(f"col('{col_str}')")
                return ", ".join(columns)
        except:
            pass
        return None

    def _extract_order_by_clause(self, parsed_sql) -> Optional[str]:
        """Extract ORDER BY clause."""
        try:
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Order
            ):
                order_expr = parsed_sql.find(sqlglot.expressions.Order)
                order_cols = []
                for expr in order_expr.expressions:
                    if hasattr(expr, "this"):
                        col_expr = expr.this
                        if isinstance(col_expr, sqlglot.expressions.Column):
                            table = (
                                col_expr.table
                                if hasattr(col_expr, "table") and col_expr.table
                                else None
                            )
                            column = (
                                col_expr.name
                                if hasattr(col_expr, "name")
                                else str(col_expr)
                            )
                            column = column.strip('"').strip("'")

                            if table:
                                col_ref = f"col('{table}.{column}')"
                            else:
                                col_ref = f"col('{column}')"
                        else:
                            col_str = str(col_expr).strip('"').strip("'")
                            col_ref = f"col('{col_str}')"

                        # Check for DESC
                        if hasattr(expr, "args") and expr.args.get("desc"):
                            col_ref += ".desc()"
                        else:
                            col_ref += ".asc()"

                        order_cols.append(col_ref)
                return ", ".join(order_cols)
        except:
            pass
        return None

    def _convert_where_to_filter(self, where_str: str) -> str:
        """Convert SQL WHERE clause to PySpark filter."""
        # Simple conversions - can be enhanced
        filter_str = where_str
        filter_str = re.sub(
            r'\b(\w+)\s*=\s*([\'"]?)([^\'"\s]+)\2', r"col('\1') == '\3'", filter_str
        )
        filter_str = re.sub(r"\b(\w+)\s*>\s*([0-9]+)", r"col('\1') > \2", filter_str)
        filter_str = re.sub(r"\b(\w+)\s*<\s*([0-9]+)", r"col('\1') < \2", filter_str)
        filter_str = re.sub(r"\bAND\b", " & ", filter_str)
        filter_str = re.sub(r"\bOR\b", " | ", filter_str)
        return filter_str

    def _generate_dialect_optimizations(
        self, parsed_sql, table_info: Optional[Dict] = None, dialect: str = "spark"
    ) -> List[str]:
        """Generate dialect-specific optimization suggestions."""
        optimizations = []

        try:
            # Dialect-specific optimizations
            if dialect == "postgres":
                optimizations.extend(self._get_postgres_optimizations(parsed_sql))
            elif dialect == "oracle":
                optimizations.extend(self._get_oracle_optimizations(parsed_sql))
            elif dialect == "redshift":
                optimizations.extend(self._get_redshift_optimizations(parsed_sql))

            # General optimizations based on query structure
            if hasattr(parsed_sql, "find_all"):
                # Check for joins
                joins = list(parsed_sql.find_all(sqlglot.expressions.Join))
                if joins:
                    optimizations.append(
                        "Consider using broadcast joins for small tables (< 200MB)"
                    )
                    optimizations.append(
                        "Ensure join keys are properly partitioned and bucketed"
                    )
                    if len(joins) > 2:
                        optimizations.append(
                            "Complex joins detected - consider breaking into multiple steps"
                        )

                # Check for CTEs
                ctes = list(parsed_sql.find_all(sqlglot.expressions.CTE))
                if ctes:
                    optimizations.append(
                        "CTEs detected - consider caching intermediate results"
                    )
                    optimizations.append("Materialize CTEs if used multiple times")

                # Check for window functions
                windows = list(parsed_sql.find_all(sqlglot.expressions.Window))
                if windows:
                    optimizations.append(
                        "Window functions detected - ensure proper partitioning"
                    )
                    optimizations.append(
                        "Consider using rangeBetween for performance if applicable"
                    )

                # Check for subqueries
                subqueries = list(parsed_sql.find_all(sqlglot.expressions.Subquery))
                if subqueries:
                    optimizations.append(
                        "Subqueries detected - consider converting to joins where possible"
                    )

            # Check for aggregations
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Group
            ):
                optimizations.append(
                    "Consider pre-aggregating data if this query runs frequently"
                )
                optimizations.append(
                    "Use appropriate number of shuffle partitions for groupBy operations"
                )

            # Check for ORDER BY
            if hasattr(parsed_sql, "find") and parsed_sql.find(
                sqlglot.expressions.Order
            ):
                optimizations.append(
                    "ORDER BY operations are expensive - consider if full sorting is necessary"
                )
                optimizations.append(
                    "Use coalesce(1) before orderBy if small result set expected"
                )

            # General performance optimizations
            optimizations.extend(
                [
                    "Use column pruning - select only needed columns early in the pipeline",
                    "Apply filters as early as possible (predicate pushdown)",
                    "Consider caching DataFrames that are reused multiple times",
                    "Use appropriate file formats (Parquet recommended, Delta for ACID)",
                    "Set appropriate spark.sql.adaptive.enabled=true for adaptive query execution",
                ]
            )

        except Exception as e:
            self.logger.warning(f"Error generating optimizations: {str(e)}")
            optimizations.append(
                "Unable to generate specific optimizations - manual review recommended"
            )

        return optimizations

    def _get_postgres_optimizations(self, parsed_sql) -> List[str]:
        """PostgreSQL-specific optimization suggestions."""
        optimizations = []

        # Check for PostgreSQL-specific functions
        sql_str = str(parsed_sql).lower()

        if (
            "string_agg" in sql_str
            or "array_agg" in sql_str
            or "group_concat" in sql_str
        ):
            optimizations.append(
                "PostgreSQL aggregation functions converted to collect_list - consider using appropriate partitioning"
            )

        if "extract(" in sql_str:
            optimizations.append(
                "PostgreSQL EXTRACT function converted - ensure date columns are properly typed"
            )

        if "regexp_replace" in sql_str:
            optimizations.append(
                "Regular expression operations can be expensive - consider pre-filtering data"
            )

        return optimizations

    def _get_oracle_optimizations(self, parsed_sql) -> List[str]:
        """Oracle-specific optimization suggestions."""
        optimizations = []

        sql_str = str(parsed_sql).lower()

        if "rownum" in sql_str:
            optimizations.append(
                "Oracle ROWNUM converted to row_number() - ensure proper window partitioning"
            )

        if "decode(" in sql_str:
            optimizations.append(
                "Oracle DECODE converted to CASE WHEN - consider using broadcast variables for large lookup tables"
            )

        if "nvl(" in sql_str or "nvl2(" in sql_str:
            optimizations.append(
                "Oracle NULL handling functions converted - consider using coalesce for better performance"
            )

        if "dual" in sql_str:
            optimizations.append(
                "Oracle DUAL table references removed - use Spark's built-in functions instead"
            )

        return optimizations

    def _get_redshift_optimizations(self, parsed_sql) -> List[str]:
        """Redshift-specific optimization suggestions."""
        optimizations = []

        sql_str = str(parsed_sql).lower()

        if "dateadd(" in sql_str or "datediff(" in sql_str:
            optimizations.append(
                "Redshift date functions converted - ensure date columns use appropriate data types"
            )

        if "isnull(" in sql_str:
            optimizations.append(
                "Redshift ISNULL converted to coalesce - consider null handling strategy"
            )

        if "convert(" in sql_str:
            optimizations.append(
                "Redshift CONVERT function converted to cast - ensure data type compatibility"
            )

        # Redshift-specific performance tips
        optimizations.extend(
            [
                "Consider using columnar storage formats (Parquet) for better compression",
                "Use appropriate partitioning strategy based on Redshift distribution keys",
                "Consider bucketing on frequently joined columns",
            ]
        )

        return optimizations

    def _enhanced_fallback_conversion(
        self, sql: str, dialect: str
    ) -> Tuple[str, List[str]]:
        """Enhanced fallback conversion with detailed guidance."""

        guidance = []

        # Analyze SQL for specific constructs that might cause issues
        sql_lower = sql.lower()

        if "with " in sql_lower and " as (" in sql_lower:
            guidance.append(
                "CTE (WITH clause) detected - consider breaking into multiple DataFrames"
            )

        if any(
            func in sql_lower
            for func in ["row_number()", "rank()", "dense_rank()", "lag(", "lead("]
        ):
            guidance.append(
                "Window functions detected - ensure proper partitioning in PySpark"
            )

        if sql_lower.count("select") > 1:
            guidance.append(
                "Multiple SELECT statements detected - consider using subqueries or CTEs"
            )

        if any(
            join in sql_lower
            for join in ["left join", "right join", "full join", "inner join"]
        ):
            guidance.append(
                "JOIN operations detected - consider broadcast joins for small tables"
            )

        # Dialect-specific guidance
        if dialect == "postgres":
            if any(func in sql_lower for func in ["string_agg", "array_agg"]):
                guidance.append(
                    "PostgreSQL aggregation functions - use collect_list() in PySpark"
                )
        elif dialect == "oracle":
            if "rownum" in sql_lower:
                guidance.append(
                    "Oracle ROWNUM - use row_number() window function in PySpark"
                )
            if "decode(" in sql_lower:
                guidance.append("Oracle DECODE - convert to CASE WHEN statements")
        elif dialect == "redshift":
            if any(func in sql_lower for func in ["dateadd", "datediff"]):
                guidance.append("Redshift date functions - use PySpark date functions")

        # Generate fallback code with better structure
        code_lines = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import *",
            "from pyspark.sql.window import Window",
            "",
            f"# FALLBACK CONVERSION - {dialect.upper()} SQL",
            "# This conversion uses direct SQL execution",
            "# Consider converting to DataFrame operations for better optimization",
            "",
            "spark = SparkSession.builder.appName('SQLToPySpark').getOrCreate()",
            "",
        ]

        # Add guidance as comments
        if guidance:
            code_lines.append("# CONVERSION GUIDANCE:")
            for guide in guidance:
                code_lines.append(f"# - {guide}")
            code_lines.append("")

        # Add the SQL execution
        code_lines.extend(
            [
                "# Original SQL execution",
                "# TODO: Convert to DataFrame operations for better performance",
                "result_df = spark.sql('''",
                f"{sql}",
                "''')",
                "",
                "# Alternative: Break down into DataFrame operations",
                "# Step 1: Load tables",
                "# table_df = spark.table('your_table')",
                "# Step 2: Apply transformations",
                "# result_df = table_df.filter(...).select(...).groupBy(...)",
                "",
                "result_df.show()",
            ]
        )

        return "\n".join(code_lines), guidance
