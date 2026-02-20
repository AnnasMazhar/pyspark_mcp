"""
Test suite for SQL to PySpark conversion fixes
Tests all the issues found and fixed during development
"""

import pytest
from pyspark_tools.server import _convert_sql_to_pyspark_internal


class TestSQLConversionFixes:
    """Test all SQL conversion fixes"""

    def test_or_conditions_in_where(self):
        """Test OR conditions with parentheses in WHERE clause"""
        sql = """SELECT * FROM t 
        WHERE (name LIKE 'A%' OR name LIKE 'B%') AND status = 1"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        # Check OR operator and LIKE are present (lit() wrapper is acceptable)
        assert ".like(" in code
        assert " | " in code
        assert "& " in code

    def test_count_distinct(self):
        """Test COUNT DISTINCT conversion"""
        sql = "SELECT COUNT(DISTINCT user_id) AS unique_users FROM t"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "countDistinct(col('user_id'))" in code

    def test_multi_condition_joins(self):
        """Test multi-condition joins with AND operator"""
        sql = """SELECT * FROM t1 
        JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "col('t1.a') == col('t2.a')" in code
        assert "col('t1.b') == col('t2.b')" in code
        assert "col('t1.c') == col('t2.c')" in code
        assert "&" in code

    def test_left_join_detection(self):
        """Test LEFT JOIN is correctly identified"""
        sql = "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "'left'" in code

    def test_group_by_numeric_references(self):
        """Test GROUP BY with numeric column references (1, 2, 3)"""
        sql = """SELECT name, COUNT(*) as cnt 
        FROM t 
        GROUP BY 1 
        ORDER BY 2 DESC"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "groupBy(col('name'))" in code
        # ORDER BY 2 should resolve to 'cnt' column
        assert "orderBy(" in code
        assert ".desc()" in code

    def test_case_when_expressions(self):
        """Test CASE WHEN expression conversion"""
        sql = """SELECT 
            CASE WHEN status = 1 THEN 'Active' ELSE 'Inactive' END as status_text
        FROM t"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "when(" in code
        assert ".otherwise(" in code

    def test_arithmetic_operations(self):
        """Test arithmetic operations (/, *, +, -)"""
        sql = "SELECT amount / 100.0 as dollars FROM t"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "col('amount') / lit(100.0)" in code

    def test_cast_expressions(self):
        """Test CAST expression conversion"""
        sql = "SELECT CAST(amount AS DECIMAL(10,2)) as amount_decimal FROM t"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert ".cast('decimal(10,2)')" in code

    def test_round_function(self):
        """Test ROUND function conversion"""
        sql = "SELECT ROUND(amount / 100.0, 2) as rounded FROM t"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "round(" in code
        assert "lit(2)" in code

    def test_nullif_function(self):
        """Test NULLIF function conversion"""
        sql = "SELECT amount / NULLIF(count, 0) as avg FROM t"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "when(col('count') == lit(0), lit(None))" in code
        assert ".otherwise(col('count'))" in code

    def test_literal_in_join_condition(self):
        """Test literal values in JOIN conditions use lit()"""
        sql = """SELECT * FROM t1 
        JOIN t2 ON t1.id = t2.id AND t2.type = 'ACTIVE'"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "lit('ACTIVE')" in code
        assert "col('t2.type') == lit('ACTIVE')" in code

    def test_complex_case_with_cast_and_arithmetic(self):
        """Test complex CASE with CAST and arithmetic operations"""
        sql = """SELECT 
            SUM(CASE WHEN status = 1 THEN CAST(amount AS DECIMAL(10,2)) / 100.0 ELSE 0 END) as total
        FROM t"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "when(" in code
        assert ".cast('decimal(10,2)')" in code
        assert "/ lit(100.0)" in code
        assert ".otherwise(lit(0))" in code

    def test_no_spurious_having_clause(self):
        """Test that ORDER BY doesn't create spurious HAVING clause"""
        sql = """SELECT name, COUNT(*) as cnt 
        FROM t 
        GROUP BY name 
        ORDER BY cnt DESC"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        # Should have orderBy but not a HAVING clause comment
        assert "orderBy(col('cnt').desc())" in code
        # Count HAVING comments - should be 0
        having_count = code.count("# HAVING clause")
        assert having_count == 0, f"Found {having_count} spurious HAVING clauses"

    def test_complex_real_world_query(self):
        """Test complex real-world query with multiple features"""
        sql = """SELECT 
            n.name AS scheme_name, 
            COUNT(DISTINCT cd.isrnd) AS unique_cards
        FROM product_account_commondata AS cd
        INNER JOIN ipe_name AS n ON cd.id = n.id
        WHERE cd.state = 1 AND n.name LIKE 'Portsmouth%'
        GROUP BY n.name
        ORDER BY unique_cards DESC"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        # Check all key features
        assert "countDistinct(col('cd.isrnd'))" in code
        assert "col('cd.id') == col('n.id')" in code
        assert ".like(" in code and "Portsmouth%" in code
        assert "groupBy(col('n.name'))" in code
        assert "orderBy(col('unique_cards').desc())" in code

    def test_is_null_and_is_not_null(self):
        """Test IS NULL and IS NOT NULL conversion"""
        sql = "SELECT * FROM t WHERE a IS NOT NULL AND b IS NULL"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "col('a').isNotNull()" in code
        assert "col('b').isNull()" in code
        assert "&" in code

    def test_alias_reference_in_same_select(self):
        """Test column alias references within the same SELECT clause"""
        sql = """SELECT 
            SUM(amount) as total,
            total * 100 as percentage
        FROM t"""

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        # The alias 'total' should be resolved to the actual expression
        assert "sum(col('amount'))" in code
        # Should appear twice - once for 'total' and once for 'percentage'
        assert code.count("sum(col('amount'))") >= 2
        # Should not have col('total')
        assert "col('total')" not in code

    def test_cast_in_join_condition(self):
        """Test CAST expressions in JOIN conditions"""
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = CAST(t2.id AS INTEGER)"

        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        code = result["pyspark_code"]

        assert "col('t1.id') == col('t2.id').cast('int')" in code
        # Should not have unparsed CAST string
        assert "CAST(" not in code or "TRY_CAST(" not in code
