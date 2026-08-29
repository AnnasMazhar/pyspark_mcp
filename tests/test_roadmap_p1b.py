"""P1 week-2 regressions: advertised surface, batch report, packaging.

Written before the production changes so they fail on main @ 93f7511
(14-tool README, batch_dir has no converted/fallback/report.json,
pyproject description lacks the pyspark-mcp disambiguation).
"""

from __future__ import annotations

import ast
import json
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PRIMARY_TOOLS = ("convert", "glue_job", "review")
LEGACY_TOOLS = (
    "analyze",
    "optimize",
    "glue_schema",
    "glue_s3",
    "glue_data",
    "refactor",
    "search",
    "context",
    "batch_status",
    "s3_source",
    "analytics",
)


def _section(text: str, heading: str) -> str:
    if heading not in text:
        return ""
    body = text.split(heading, 1)[1]
    return re.split(r"\n## ", body, maxsplit=1)[0]


class TestP13AdvertisedSurface:
    """P1-3: three primary tools; the rest marked deprecated but still registered."""

    def test_readme_lists_three_primary_tools(self):
        readme = (ROOT / "README.md").read_text()
        tools = _section(readme, "## Tools")
        assert tools.strip(), "README missing ## Tools"
        for name in PRIMARY_TOOLS:
            assert f"`{name}`" in tools, f"primary tool {name} missing from README Tools"
        assert "deprecated" in tools.lower() or "legacy" in tools.lower()
        # One screen: keep the advertised section short.
        assert len(tools.strip().splitlines()) <= 40, (
            f"Tools section is {len(tools.strip().splitlines())} lines"
        )

    def test_readme_marks_legacy_routers_deprecated(self):
        readme = (ROOT / "README.md").read_text()
        tools = _section(readme, "## Tools")
        blob = tools.lower()
        for name in LEGACY_TOOLS:
            assert name in blob, f"legacy router {name} should still be named (deprecated)"

    def test_api_docs_match_three_primary_surface(self):
        api = (ROOT / "docs" / "api.md").read_text()
        assert "deprecated" in api.lower() or "legacy" in api.lower()
        for name in PRIMARY_TOOLS:
            assert f"`{name}`" in api or f"## `{name}`" in api

    def test_legacy_router_docstrings_say_deprecated(self):
        from pyspark_tools import consolidated_tools as ct

        for name in LEGACY_TOOLS:
            fn = getattr(ct, name)
            doc = (fn.__doc__ or "").lower()
            assert "deprecated" in doc, f"{name} docstring missing deprecated"

    def test_legacy_routers_still_registered(self):
        from pyspark_tools.consolidated_tools import analyze, convert

        result = analyze(mode="sql_context", sql_content="SELECT 1")
        assert result.get("status") == "success"
        result = convert(mode="sql", sql_query="SELECT 1")
        assert result.get("status") == "success"


class TestP15BatchConversionReport:
    """P1-5: batch_dir returns converted/fallback/errors and writes report.json."""

    def test_batch_dir_report_counts_one_recursive_fallback(self, tmp_path, monkeypatch):
        from pyspark_tools import server as s
        from pyspark_tools.batch_processor import BatchProcessor
        from pyspark_tools.consolidated_tools import convert

        src = tmp_path / "sql"
        src.mkdir()
        (src / "users.sql").write_text("SELECT id, name FROM users WHERE active = 1;")
        (src / "orders.sql").write_text(
            "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id;"
        )
        (src / "join.sql").write_text(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id;"
        )
        (src / "fromless.sql").write_text("SELECT 1;")
        (src / "recursive.sql").write_text(
            """
            WITH RECURSIVE tree AS (
                SELECT id, parent_id FROM org WHERE parent_id IS NULL
                UNION ALL
                SELECT o.id, o.parent_id FROM org o JOIN tree t ON o.parent_id = t.id
            )
            SELECT * FROM tree;
            """
        )
        out = tmp_path / "out"
        bp = BatchProcessor(
            memory_manager=s.memory,
            sql_converter=s.converter,
            allowed_root=str(tmp_path),
            base_output_dir=str(out),
        )
        monkeypatch.setattr(s, "batch_processor", bp)

        result = convert(
            mode="batch_dir",
            directory_path=str(src),
            output_dir=str(out),
            recursive=False,
        )
        assert result.get("status") == "success", result
        assert result.get("converted") == 5, result
        assert result.get("fallback") == 1, result
        assert result.get("errors") == [] or result.get("errors") == [], result
        files = result.get("files") or []
        assert len(files) == 5, files
        fallback_files = [f for f in files if f.get("fallback_used")]
        assert len(fallback_files) == 1
        for item in files:
            assert "dialect" in item or "dialect_used" in item
            assert "output_path" in item
            assert item.get("fallback_used") in {True, False}
            if not item.get("fallback_used"):
                py = Path(item["output_path"])
                assert py.is_file(), item
                ast.parse(py.read_text())

        report_path = out / "report.json"
        assert report_path.is_file(), "batch_dir must write report.json next to outputs"
        report = json.loads(report_path.read_text())
        assert report.get("converted") == 5
        assert report.get("fallback") == 1
        assert "errors" in report


class TestP16PackagingSeo:
    """P1-6: PyPI description disambiguates pyspark-mcp; keywords set."""

    def test_pyproject_description_disambiguates_pyspark_mcp(self):
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        desc = data["project"]["description"]
        first = desc.split("\n\n", 1)[0]
        assert "pyspark-mcp" in first.lower()
        assert "not the live-spark" in first.lower() or "not the live-Spark" in desc
        assert "sql" in first.lower() and (
            "pyspark" in first.lower() or "glue" in first.lower()
        )

    def test_pyproject_keywords(self):
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        keywords = data["project"].get("keywords") or []
        for kw in ("sqlglot", "aws-glue", "mcp", "sql-migration"):
            assert kw in keywords, f"missing keyword {kw}: {keywords}"

    def test_readme_keeps_pyspark_mcp_disambiguation_box(self):
        readme = (ROOT / "README.md").read_text()
        doesnt = _section(readme, "## What It Doesn't Do")
        assert "pyspark-mcp" in readme.lower()
        assert "SemyonSinchenko" in readme
        assert "live" in readme.lower()
        assert doesnt.strip()
