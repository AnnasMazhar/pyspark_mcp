"""Regression tests for verified round-2 defects.

Defect 2: `{schema}` placeholders must not yield status=success with mangled
PySpark (`col(':schema.id')`, `::bit(`).
Defect 3: JOIN ON ... OR ... must not emit a bare `True` join predicate.
"""

import ast
import re

from pyspark_tools.consolidated_tools import convert
from pyspark_tools.server import _convert_sql_to_pyspark_internal
from pyspark_tools.sql_converter import SQLToPySparkConverter


def _join_predicates(code: str):
    """Return the middle argument of each `.join(df, PRED, how)` call."""
    preds = []
    for line in code.splitlines():
        stripped = line.strip()
        if ".join(" not in stripped:
            continue
        # .join(x_df.alias('x'), PRED, 'inner')
        match = re.search(r"\.join\([^,]+,\s*(.+),\s*'[^']+'\)", stripped)
        if match:
            preds.append(match.group(1).strip())
    return preds


class TestPlaceholderHandling:
    def test_schema_placeholder_does_not_mangle_output(self):
        sql = "SELECT {schema}.id::bit(32)::int AS id FROM {schema}.users"
        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")

        assert result.get("status") in {"success", "error"}
        if result.get("status") == "error":
            message = (result.get("message") or "") + " ".join(
                result.get("warnings") or []
            )
            assert "placeholder" in message.lower()
            return

        code = result.get("pyspark_code") or ""
        assert code.strip()
        ast.parse(code)
        assert ":schema." not in code
        assert "::bit(" not in code
        assert "cast('bit(" not in code.lower()
        assert result.get("dialect_used") == "postgres"
        assert result.get("fallback_used") is False

    def test_convert_router_schema_placeholder(self):
        sql = "SELECT {schema}.id::bit(32)::int AS id FROM {schema}.users"
        result = convert(mode="sql", sql_query=sql, dialect="postgres")
        assert result.get("status") in {"success", "error"}
        if result.get("status") == "success":
            code = result["pyspark_code"]
            ast.parse(code)
            assert ":schema." not in code
            assert "::bit(" not in code
            assert "cast('bit(" not in code.lower()

    def test_table_name_and_database_placeholders(self):
        sql = "SELECT * FROM {database}.{table_name} WHERE id > 1"
        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        assert result.get("status") in {"success", "error"}
        if result.get("status") == "success":
            code = result["pyspark_code"]
            ast.parse(code)
            assert "{database}" not in code
            assert "{table_name}" not in code
            assert ":database" not in code

    def test_clean_sql_unchanged(self):
        """Placeholder handling must not regress standard postgres SQL."""
        sql = "SELECT id::int, name::TEXT FROM users WHERE id IS NOT NULL"
        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        assert result.get("status") == "success"
        code = result["pyspark_code"]
        ast.parse(code)
        assert "isNotNull()" in code
        assert result.get("fallback_used") is False
        assert result.get("dialect_used") == "postgres"


class TestOrJoinPredicate:
    def test_or_join_keeps_predicate(self):
        sql = "SELECT * FROM a JOIN b ON a.id = b.id OR a.id2 = b.id2"
        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        assert result.get("status") == "success"
        code = result["pyspark_code"]
        ast.parse(code)
        preds = _join_predicates(code)
        assert preds, f"no join line in:\n{code}"
        for pred in preds:
            assert pred != "True", f"OR join collapsed to True:\n{code}"
        assert "|" in code
        assert "col('a.id')" in code
        assert "col('b.id')" in code
        assert "col('a.id2')" in code
        assert "col('b.id2')" in code

    def test_and_join_still_works(self):
        sql = (
            "SELECT * FROM orders o JOIN customers c "
            "ON o.customer_id = c.id AND c.active = 1"
        )
        result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        assert result.get("status") == "success"
        code = result["pyspark_code"]
        ast.parse(code)
        assert "col('o.customer_id') == col('c.id')" in code
        assert "col('c.active') == lit(1)" in code
        assert "&" in code
        for pred in _join_predicates(code):
            assert pred != "True"

    def test_left_inner_and_three_table_joins(self):
        cases = [
            "SELECT * FROM a JOIN b ON a.id = b.id",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.b_id",
        ]
        for sql in cases:
            result = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
            assert result.get("status") == "success", sql
            code = result["pyspark_code"]
            ast.parse(code)
            for pred in _join_predicates(code):
                assert pred != "True", f"{sql}\n{code}"

    def test_converter_direct_or_join(self):
        converter = SQLToPySparkConverter()
        result = converter.convert_sql_to_pyspark(
            "SELECT * FROM a JOIN b ON a.id = b.id OR a.id2 = b.id2",
            dialect="postgres",
        )
        assert result.success is True
        assert "True" not in [
            p.strip() for p in _join_predicates(result.pyspark_code)
        ]
        assert "|" in result.pyspark_code
