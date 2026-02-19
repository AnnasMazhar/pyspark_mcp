"""Tests for enhanced SQL converter with dialect support."""

import pytest

from pyspark_tools.sql_converter import ConversionResult, SQLToPySparkConverter


class TestEnhancedSQLConverter:
    """Test enhanced SQL converter functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.converter = SQLToPySparkConverter()

    def test_dialect_detection(self):
        """Test automatic dialect detection."""
        # PostgreSQL
        postgres_sql = "SELECT string_agg(name, ',') FROM users"
        result = self.converter.convert_sql_to_pyspark(postgres_sql)
        assert result.dialect_used == "postgres"

        # Oracle
        oracle_sql = "SELECT nvl(name, 'Unknown') FROM dual"
        result = self.converter.convert_sql_to_pyspark(oracle_sql)
        assert result.dialect_used == "oracle"

        # Redshift
        redshift_sql = "SELECT dateadd(day, 1, created_date) FROM orders"
        result = self.converter.convert_sql_to_pyspark(redshift_sql)
        assert result.dialect_used == "redshift"

    def test_explicit_dialect_specification(self):
        """Test explicit dialect specification."""
        sql = "SELECT name FROM users"
        result = self.converter.convert_sql_to_pyspark(sql, dialect="postgres")
        assert result.dialect_used == "postgres"

    def test_cte_handling(self):
        """Test Common Table Expression handling."""
        cte_sql = """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT * FROM user_stats WHERE order_count > 5
        """

        result = self.converter.convert_sql_to_pyspark(cte_sql)
        assert isinstance(result, ConversionResult)
        assert any("CTEs" in construct for construct in result.complex_constructs)
        assert "CTE" in result.pyspark_code

    def test_window_functions(self):
        """Test window function conversion."""
        window_sql = """
        SELECT 
            name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
        FROM employees
        """

        result = self.converter.convert_sql_to_pyspark(window_sql)
        assert any(
            "Window functions" in construct for construct in result.complex_constructs
        )
        assert "Window" in result.pyspark_code
        assert "row_number" in result.pyspark_code.lower()

    def test_complex_subqueries(self):
        """Test complex subquery handling."""
        subquery_sql = """
        SELECT u.name, 
               (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
        FROM users u
        WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100)
        """

        result = self.converter.convert_sql_to_pyspark(subquery_sql)
        assert any("Subqueries" in construct for construct in result.complex_constructs)

    def test_postgres_function_mapping(self):
        """Test PostgreSQL function mapping."""
        postgres_sql = "SELECT string_agg(name, ','), array_agg(id) FROM users"
        result = self.converter.convert_sql_to_pyspark(postgres_sql, dialect="postgres")

        assert result.dialect_used == "postgres"
        assert "collect_list" in result.pyspark_code
        assert any("PostgreSQL" in opt for opt in result.optimizations)

    def test_oracle_function_mapping(self):
        """Test Oracle function mapping."""
        oracle_sql = """
        SELECT 
            nvl(name, 'Unknown'),
            decode(status, 1, 'Active', 2, 'Inactive', 'Unknown'),
            rownum
        FROM users
        """

        result = self.converter.convert_sql_to_pyspark(oracle_sql, dialect="oracle")
        assert result.dialect_used == "oracle"
        assert "coalesce" in result.pyspark_code or "when" in result.pyspark_code
        assert "row_number" in result.pyspark_code

    def test_redshift_function_mapping(self):
        """Test Redshift function mapping."""
        redshift_sql = """
        SELECT 
            dateadd(day, 1, created_date),
            datediff(day, created_date, updated_date),
            isnull(name, 'Unknown')
        FROM orders
        """

        result = self.converter.convert_sql_to_pyspark(redshift_sql, dialect="redshift")
        assert result.dialect_used == "redshift"
        assert "date_add" in result.pyspark_code or "datediff" in result.pyspark_code

    def test_complex_joins(self):
        """Test complex join handling."""
        join_sql = """
        SELECT u.name, o.amount, p.name as product_name
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        INNER JOIN products p ON o.product_id = p.id
        RIGHT JOIN categories c ON p.category_id = c.id
        """

        result = self.converter.convert_sql_to_pyspark(join_sql)
        assert any(
            "Complex joins" in construct for construct in result.complex_constructs
        )
        assert any("broadcast" in opt.lower() for opt in result.optimizations)

    def test_fallback_conversion(self):
        """Test fallback conversion with guidance."""
        # Intentionally complex/problematic SQL
        complex_sql = """
        WITH RECURSIVE hierarchy AS (
            SELECT id, parent_id, name, 1 as level
            FROM categories
            WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.parent_id, c.name, h.level + 1
            FROM categories c
            JOIN hierarchy h ON c.parent_id = h.id
        )
        SELECT * FROM hierarchy
        """

        result = self.converter.convert_sql_to_pyspark(complex_sql)
        assert result.fallback_used
        assert len(result.warnings) > 0
        assert "spark.sql" in result.pyspark_code

    def test_dialect_specific_optimizations(self):
        """Test dialect-specific optimization suggestions."""
        # PostgreSQL optimizations
        postgres_result = self.converter.convert_sql_to_pyspark(
            "SELECT string_agg(name, ',') FROM users", dialect="postgres"
        )
        assert any("collect_list" in opt for opt in postgres_result.optimizations)

        # Oracle optimizations
        oracle_result = self.converter.convert_sql_to_pyspark(
            "SELECT rownum, name FROM users", dialect="oracle"
        )
        assert any("row_number" in opt for opt in oracle_result.optimizations)

        # Redshift optimizations
        redshift_result = self.converter.convert_sql_to_pyspark(
            "SELECT dateadd(day, 1, created_date) FROM orders", dialect="redshift"
        )
        assert any("columnar" in opt.lower() for opt in redshift_result.optimizations)

    def test_conversion_result_structure(self):
        """Test ConversionResult structure."""
        sql = "SELECT name, COUNT(*) FROM users GROUP BY name"
        result = self.converter.convert_sql_to_pyspark(sql)

        assert isinstance(result, ConversionResult)
        assert hasattr(result, "pyspark_code")
        assert hasattr(result, "optimizations")
        assert hasattr(result, "warnings")
        assert hasattr(result, "dialect_used")
        assert hasattr(result, "complex_constructs")
        assert hasattr(result, "fallback_used")

        assert isinstance(result.pyspark_code, str)
        assert isinstance(result.optimizations, list)
        assert isinstance(result.warnings, list)
        assert isinstance(result.complex_constructs, list)
        assert isinstance(result.fallback_used, bool)

    def test_case_statement_handling(self):
        """Test CASE statement conversion."""
        case_sql = """
        SELECT 
            name,
            CASE 
                WHEN age < 18 THEN 'Minor'
                WHEN age < 65 THEN 'Adult'
                ELSE 'Senior'
            END as age_group
        FROM users
        """

        result = self.converter.convert_sql_to_pyspark(case_sql)
        assert any(
            "CASE statements" in construct for construct in result.complex_constructs
        )
        assert "when" in result.pyspark_code.lower()

    def test_multiple_aggregations(self):
        """Test multiple aggregation handling."""
        agg_sql = """
        SELECT 
            department,
            COUNT(*) as emp_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY department
        """

        result = self.converter.convert_sql_to_pyspark(agg_sql)
        assert any(
            "Multiple aggregations" in construct
            for construct in result.complex_constructs
        )
        assert any("groupBy" in opt for opt in result.optimizations)

    def test_error_handling(self):
        """Test error handling and graceful degradation."""
        # Test with malformed SQL
        malformed_sql = "SELECT FROM WHERE"
        result = self.converter.convert_sql_to_pyspark(malformed_sql)

        # Should still return a result, possibly with fallback
        assert isinstance(result, ConversionResult)
        assert len(result.warnings) > 0 or result.fallback_used

    def test_unsupported_dialect_fallback(self):
        """Test fallback when unsupported dialect is specified."""
        sql = "SELECT name FROM users"
        result = self.converter.convert_sql_to_pyspark(sql, dialect="unsupported_db")

        # Should fallback to spark dialect
        assert result.dialect_used == "spark"

    def test_performance_optimizations(self):
        """Test performance optimization suggestions."""
        complex_sql = """
        SELECT u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_date > '2023-01-01'
        GROUP BY u.name
        ORDER BY order_count DESC
        """

        result = self.converter.convert_sql_to_pyspark(complex_sql)

        # Should have various optimization suggestions
        opt_text = " ".join(result.optimizations).lower()
        assert "broadcast" in opt_text or "join" in opt_text
        assert "partition" in opt_text or "cache" in opt_text
        assert "column pruning" in opt_text or "predicate pushdown" in opt_text
