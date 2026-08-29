"""P1 week-1 regressions: demo examples + production-shaped code style.

Written before the production changes so they fail on main @ 93f7511
(no examples/, no spark_sum alias, no target=glue preamble, no Try it).
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README_SQL = (
    "SELECT o.customer_id, c.name, SUM(o.amount) AS total "
    "FROM orders o JOIN customers c ON o.customer_id = c.id "
    "WHERE o.status = 'paid' GROUP BY o.customer_id, c.name"
)


def _readme_example_blocks() -> tuple[str, str]:
    text = (ROOT / "README.md").read_text()
    section = text.split("## Example: SQL → PySpark", 1)[1]
    section = re.split(r"\n## ", section, maxsplit=1)[0]
    sql = re.search(r"```sql\n(.*?)```", section, re.S)
    py = re.search(r"```python\n(.*?)```", section, re.S)
    assert sql and py, "README Example section missing sql/python fences"
    return sql.group(1).strip(), py.group(1).strip()


class TestP12ExamplesAndTryIt:
    """P1-2: examples/ + captured output + short README Try it."""

    def test_example_sql_files_exist(self):
        examples = ROOT / "examples"
        assert (examples / "postgres_orders.sql").is_file()
        assert (examples / "oracle_decode.sql").is_file()

    def test_captured_outputs_exist(self):
        out = ROOT / "examples" / "out"
        assert (out / "postgres_orders.py").is_file()
        assert (out / "oracle_decode.py").is_file()
        glue = list(out.glob("*glue*.py"))
        assert glue, "examples/out/ must include a captured Glue template"

    def test_captured_outputs_match_converter(self):
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        converter = SQLToPySparkConverter()
        pairs = [
            ("postgres_orders.sql", "postgres_orders.py", "postgres"),
            ("oracle_decode.sql", "oracle_decode.py", "oracle"),
        ]
        for sql_name, py_name, dialect in pairs:
            sql = (ROOT / "examples" / sql_name).read_text()
            captured = (ROOT / "examples" / "out" / py_name).read_text()
            result = converter.convert_sql_to_pyspark(sql, dialect=dialect)
            assert result.success, py_name
            ast.parse(result.pyspark_code)
            assert result.pyspark_code.strip() == captured.strip(), (
                f"{py_name} drifted from converter output"
            )

    def test_readme_try_it_section_is_short_and_uses_public_cli(self):
        readme = (ROOT / "README.md").read_text()
        assert "## Try it" in readme, "README missing ## Try it"
        section = readme.split("## Try it", 1)[1]
        section = re.split(r"\n## ", section, maxsplit=1)[0]
        lines = section.strip().splitlines()
        assert len(lines) <= 15, f"Try it has {len(lines)} lines (max 15)"
        assert "pip install pyspark-tools" in section
        assert "pyspark-tools" in section
        assert "examples/out" in section or "examples/out/" in section


class TestP14ProductionCodeStyle:
    """P1-4: production emit vs notebook flag; README snapshot."""

    def test_default_emit_has_explicit_imports_no_star_no_show(self):
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        result = SQLToPySparkConverter().convert_sql_to_pyspark(
            README_SQL, dialect="spark"
        )
        assert result.success
        code = result.pyspark_code
        ast.parse(code)
        assert "from pyspark.sql.functions import *" not in code
        assert "result_df.show()" not in code
        assert "sum as spark_sum" in code
        assert "spark_sum(" in code
        assert re.search(r"(?<![\w.])sum\(", code) is None

    def test_notebook_style_keeps_import_star_and_show(self):
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        result = SQLToPySparkConverter().convert_sql_to_pyspark(
            README_SQL, dialect="spark", style="notebook"
        )
        assert result.success
        code = result.pyspark_code
        ast.parse(code)
        assert "from pyspark.sql.functions import *" in code
        assert "result_df.show()" in code

    def test_glue_target_omits_sparksession_builder(self):
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        result = SQLToPySparkConverter().convert_sql_to_pyspark(
            README_SQL, dialect="spark", target="glue"
        )
        assert result.success
        code = result.pyspark_code
        ast.parse(code)
        assert "SparkSession.builder" not in code
        assert "from pyspark.sql import SparkSession" not in code

    def test_readme_python_is_converter_snapshot(self):
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        sql, documented = _readme_example_blocks()
        result = SQLToPySparkConverter().convert_sql_to_pyspark(sql, dialect="spark")
        assert result.success
        assert result.pyspark_code.strip() == documented, (
            "README Python is not the converter snapshot — regenerate from "
            "SQLToPySparkConverter().convert_sql_to_pyspark(..., dialect='spark')"
        )

    def test_convert_router_forwards_style_and_target(self):
        from pyspark_tools.consolidated_tools import convert

        nb = convert(
            mode="sql",
            sql_query=README_SQL,
            dialect="spark",
            style="notebook",
        )
        assert nb.get("status") == "success", nb
        assert "from pyspark.sql.functions import *" in nb["pyspark_code"]

        glue = convert(
            mode="sql",
            sql_query=README_SQL,
            dialect="spark",
            target="glue",
        )
        assert glue.get("status") == "success", glue
        assert "SparkSession.builder" not in glue["pyspark_code"]
