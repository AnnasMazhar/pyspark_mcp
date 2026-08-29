"""P0 launch-blocker regressions (roadmap round-3).

Each class maps to one orchestrator-verified defect. Tests were written
before the production fix so they fail on main @ 431182f.
"""

from __future__ import annotations

import ast
import json
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


class TestP01CliNameCollision:
    """P0-1: console script is pyspark-tools; pyspark-mcp is a deprecated alias."""

    def test_primary_console_script_is_pyspark_tools(self):
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        scripts = data["project"]["scripts"]
        assert "pyspark-tools" in scripts, scripts
        assert scripts["pyspark-tools"] == "pyspark_tools:main"

    def test_deprecated_alias_warns_then_runs(self, capsys):
        from unittest.mock import patch

        import pyspark_tools

        assert hasattr(pyspark_tools, "deprecated_mcp_main")
        with patch("pyspark_tools.server.app.run"):
            pyspark_tools.deprecated_mcp_main()
        err = capsys.readouterr().err
        assert "pyspark-tools" in err
        assert "SemyonSinchenko" in err
        assert "live Spark" in err

    def test_readme_does_not_advertise_pyspark_mcp_as_primary(self):
        readme = (ROOT / "README.md").read_text()
        quick_start = readme.split("## Example")[0]
        # Primary advertised command in Quick Start is pyspark-tools.
        assert "pyspark-tools" in quick_start
        assert re.search(r"^pyspark-mcp\b", quick_start, re.M) is None


class TestP02DialectAwareCache:
    """P0-2: same SQL + different dialect must not share a cache entry."""

    def test_postgres_then_spark_does_not_return_stale_postgres_cache(self):
        from pyspark_tools.server import _convert_sql_to_pyspark_internal, memory

        sql = "SELECT id::int FROM users WHERE id IS NOT NULL"
        first = _convert_sql_to_pyspark_internal(sql, dialect="postgres")
        assert first.get("status") == "success"
        assert first.get("dialect_used") == "postgres"

        second = _convert_sql_to_pyspark_internal(sql, dialect="spark")
        assert second.get("status") == "success"
        assert second.get("dialect_used") == "spark"
        if second.get("source") == "cache":
            pytest.fail(
                "spark conversion reused postgres cache: "
                f"dialect_used={second.get('dialect_used')!r}"
            )

        pg_hash = memory._hash_sql(sql, dialect="postgres")
        spark_hash = memory._hash_sql(sql, dialect="spark")
        assert pg_hash != spark_hash


class TestP03FromlessAndDottedPlaceholders:
    """P0-3: FROM-less SQL must parse; {foo.bar} substitutes or errors loudly."""

    def test_select_one_succeeds_and_parses(self):
        from pyspark_tools.consolidated_tools import convert

        result = convert(mode="sql", sql_query="SELECT 1")
        assert result.get("status") == "success", result
        code = result["pyspark_code"]
        ast.parse(code)
        # Must not emit a dangling method chain with no receiver.
        stripped = "\n".join(
            ln for ln in code.splitlines() if not ln.strip().startswith("#")
        )
        assert not re.search(r"^\s*\.select\(", stripped, re.M), code

    def test_dotted_placeholder_substitutes_or_errors_by_name(self):
        from pyspark_tools.consolidated_tools import convert

        result = convert(
            mode="sql",
            sql_query="SELECT id FROM {foo.bar}",
            dialect="postgres",
        )
        if result.get("status") == "error":
            blob = (result.get("message") or "") + " ".join(
                result.get("warnings") or []
            )
            assert "foo.bar" in blob or "{foo.bar}" in blob, blob
            return
        assert result.get("status") == "success", result
        code = result["pyspark_code"]
        ast.parse(code)
        assert "{foo.bar}" not in code
        assert ":foo" not in code
        # Substitution happened: leftover braces are gone and the result parses.


class TestP04DocsMatchProduct:
    """P0-4: README/docs/version/changelog must match the shipped product."""

    def test_version_matches_pyproject(self):
        import pyspark_tools

        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        assert pyspark_tools.__version__ == data["project"]["version"]

    def test_changelog_has_0_0_5_entry(self):
        changelog = (ROOT / "CHANGELOG.md").read_text()
        assert re.search(r"^## \[0\.0\.5\]", changelog, re.M)

    def test_no_unregistered_51_tool_names_in_docs(self):
        banned = (
            "convert_sql_to_pyspark",
            "analyze_data_flow",
            "batch_process_directory",
        )
        for path in (ROOT / "docs").glob("*.md"):
            text = path.read_text()
            for name in banned:
                assert name not in text, f"{path.name} still documents {name}"

    def test_linked_doc_files_exist(self):
        missing = []
        for md in [
            ROOT / "README.md",
            *sorted((ROOT / "docs").glob("*.md")),
            ROOT / "CONTRIBUTING.md",
        ]:
            text = md.read_text()
            for match in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", text):
                href = match.group(2)
                if href.startswith(("http://", "https://", "mailto:", "#")):
                    continue
                target = (md.parent / href.split("#")[0]).resolve()
                if not target.exists():
                    missing.append(f"{md.relative_to(ROOT)} → {href}")
        assert missing == [], "dead links: " + "; ".join(missing)

    def test_examples_dir_exists_or_is_unreferenced(self):
        examples = ROOT / "examples"
        refs = []
        for md in [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
            if (
                re.search(r"\bexamples/", md.read_text())
                or "`examples/`" in md.read_text()
            ):
                refs.append(str(md.relative_to(ROOT)))
        if refs:
            assert examples.is_dir() and any(
                examples.iterdir()
            ), f"examples/ referenced from {refs} but missing or empty"

    def test_contributing_urls_point_at_annasmazhar(self):
        text = (ROOT / "CONTRIBUTING.md").read_text()
        assert "your-org" not in text
        assert "github.com/AnnasMazhar/pyspark_mcp" in text

    def test_readme_converter_example_parses_like_real_output(self):
        """README must not advertise import * / show() as the default emit."""
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        sql = (
            "SELECT o.customer_id, c.name, SUM(o.amount) AS total "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "WHERE o.status = 'paid' GROUP BY o.customer_id, c.name"
        )
        result = SQLToPySparkConverter().convert_sql_to_pyspark(sql, dialect="spark")
        assert result.success is True
        ast.parse(result.pyspark_code)
        assert "from pyspark.sql.functions import *" not in result.pyspark_code
        assert "result_df.show()" not in result.pyspark_code


class TestP05HonestOptimizeAndGlueS3:
    """P0-5: optimize does not claim a rewrite; glue_s3 invents no % speedups."""

    def test_optimize_does_not_claim_rewrite(self):
        from pyspark_tools.consolidated_tools import optimize

        code = "df.filter(col('x') == 1).select('y')"
        result = optimize(mode="code", code=code)
        assert result.get("status") == "success"
        assert "suggestions" in result
        assert "optimized_code" not in result
        if "original_code" in result:
            assert result["original_code"] == code

    def test_glue_s3_analyze_has_no_invented_percent_speedups(self):
        from pyspark_tools.consolidated_tools import glue_s3

        result = glue_s3(
            mode="analyze",
            s3_location="s3://bucket/path",
            database_name="raw",
            table_name="orders",
        )
        blob = json.dumps(result)
        assert "% faster" not in blob.lower()
        assert "20-50%" not in blob
        assert "10-30%" not in blob
        assert "90%" not in blob
        assert "50-80%" not in blob


class TestP06GlueTemplateCorrectness:
    """P0-6: Glue 5.0 default, one DynamicFrame import, commit-on-success, bookmark warning."""

    def test_default_glue_version_is_5_0(self):
        from pyspark_tools.aws_glue_integration import GlueJobConfig

        assert GlueJobConfig(job_name="demo").glue_version == "5.0"

    def test_exactly_one_correct_dynamicframe_import(self):
        from pyspark_tools.aws_glue_integration import AWSGlueIntegration, GlueJobConfig

        imports = AWSGlueIntegration()._generate_imports(
            GlueJobConfig(job_name="demo", use_dynamic_frame=True)
        )
        assert "from awsglue.dynamicframe import DynamicFrame" in imports
        assert "from awsglue import DynamicFrame" not in imports
        assert imports.count("DynamicFrame") == 1

    def test_job_commit_not_under_finally(self):
        from pyspark_tools.aws_glue_integration import (
            AWSGlueIntegration,
            GlueJobConfig,
        )

        glue = AWSGlueIntegration()
        config = GlueJobConfig(job_name="demo", include_bookmarking=True)
        error_code = glue._generate_error_handling(config)
        assert "finally:" not in error_code
        assert "job.commit()" in error_code
        assert "raise e" in error_code or "raise" in error_code

        template = glue.generate_enhanced_glue_job_template(
            config, transformation_sql="SELECT 1"
        )["template"]
        ast.parse(template)
        commit_idx = template.find("job.commit()")
        finally_idx = template.find("finally:")
        assert commit_idx != -1
        if finally_idx != -1:
            assert commit_idx < finally_idx

    def test_bookmark_warning_when_spark_sql_and_bookmarks(self):
        from pyspark_tools.aws_glue_integration import (
            AWSGlueIntegration,
            GlueJobConfig,
        )

        result = AWSGlueIntegration().generate_enhanced_glue_job_template(
            GlueJobConfig(job_name="demo", include_bookmarking=True),
            transformation_sql="SELECT id FROM source_table",
        )
        blob = json.dumps(result) + result.get("template", "")
        assert "bookmark" in blob.lower()
        assert "transformation_ctx" in blob or "DynamicFrame" in blob
        assert any(
            "spark.sql" in blob and word in blob.lower()
            for word in ("do not bookmark", "don't bookmark", "cannot bookmark", "need")
        )
