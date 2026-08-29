"""Tests for consolidated tool router functions.

Each of the 14 routers is called with documented primary-mode params.
Assertions check status, output structure, and (for code-generating tools)
that emitted Python parses. Invalid-input cases check the specific error dict.
"""

import ast
import json

from pyspark_tools.consolidated_tools import (
    analytics,
    analyze,
    batch_status,
    context,
    convert,
    glue_data,
    glue_job,
    glue_s3,
    glue_schema,
    optimize,
    refactor,
    review,
    s3_source,
    search,
)


def _assert_success(result, name):
    assert isinstance(result, dict), f"{name}: expected dict, got {type(result)}"
    blob = json.dumps(result, default=str).lower()
    assert "unexpected keyword argument" not in blob, f"{name}: kwarg TypeError leaked"
    assert result.get("status") == "success", (
        f"{name}: expected status=success, got {result.get('status')!r} "
        f"message={result.get('message')!r}"
    )
    return result


def _assert_error(result, *, message_contains=None, name="router"):
    assert isinstance(result, dict), f"{name}: expected error dict"
    assert result.get("status") == "error", (
        f"{name}: expected status=error, got {result!r}"
    )
    assert "message" in result, f"{name}: error dict missing message"
    if message_contains:
        assert message_contains.lower() in str(result.get("message", "")).lower(), (
            f"{name}: message {result.get('message')!r} does not contain "
            f"{message_contains!r}"
        )
    return result


class TestConvertRouter:
    def test_convert_sql_mode(self):
        result = convert(mode="sql", sql_query="SELECT * FROM users", dialect="postgres")
        _assert_success(result, "convert")
        code = result.get("pyspark_code") or ""
        assert code.strip()
        ast.parse(code)
        assert "spark.table" in code or "users" in code
        assert "fallback_used" in result
        assert "dialect_used" in result

    def test_convert_sql_with_glue_template(self):
        result = convert(
            mode="sql",
            sql_query="SELECT id FROM users",
            include_glue_template=True,
        )
        _assert_success(result, "convert+glue")

    def test_convert_invalid_mode(self):
        result = convert(mode="invalid_mode")
        _assert_error(result, message_contains="Unknown mode", name="convert invalid")

    def test_convert_sql_requires_query(self):
        result = convert(mode="sql")
        _assert_error(result, message_contains="sql_query", name="convert missing sql")


class TestAnalyzeRouter:
    def test_analyze_sql_context(self):
        result = analyze(mode="sql_context", sql_content="SELECT * FROM users")
        _assert_success(result, "analyze")
        assert "tables" in result
        assert "dialect" in result or "schemas" in result

    def test_analyze_workspace(self):
        result = analyze(
            mode="workspace",
            sql_content="SELECT id FROM orders",
            workspace_name="demo",
        )
        assert result.get("status") in {"success", "info"}
        assert isinstance(result, dict)

    def test_analyze_invalid_mode(self):
        result = analyze(mode="nope")
        _assert_error(result, message_contains="Unknown mode", name="analyze invalid")

    def test_analyze_sql_context_requires_content(self):
        result = analyze(mode="sql_context")
        _assert_error(
            result, message_contains="sql_content", name="analyze missing sql"
        )


class TestOptimizeRouter:
    def test_optimize_code(self):
        result = optimize(mode="code", code="df.filter(col('x') == 1).select('y')")
        _assert_success(result, "optimize")
        assert "optimized_code" in result
        assert "suggestions" in result


class TestReviewRouter:
    def test_review_code(self):
        result = review(mode="code", code="df = spark.table('t')\ndf.show()")
        _assert_success(result, "review")
        assert "issues" in result
        assert "summary" in result


class TestGlueJobRouter:
    def test_glue_template(self):
        result = glue_job(mode="template", job_name="demo-job", sql_query="SELECT 1")
        _assert_success(result, "glue_job template")
        template = result.get("template") or result.get("job_template") or ""
        assert str(template).strip(), f"glue_job missing template: {result.keys()}"
        assert "job_name" in result

    def test_glue_properties_requires_name(self):
        result = glue_job(mode="properties")
        _assert_error(result, message_contains="job_name", name="glue_job properties")

    def test_glue_properties(self):
        result = glue_job(mode="properties", job_name="demo-job")
        _assert_success(result, "glue_job properties")


class TestGlueSchemaRouter:
    def test_glue_detect(self):
        result = glue_schema(
            mode="detect", sample_data=[{"id": 1, "name": "a"}], table_name="users"
        )
        _assert_success(result, "glue_schema")
        assert "detected_schema" in result
        assert result.get("table_name") == "users"


class TestGlueS3Router:
    def test_glue_s3_analyze(self):
        result = glue_s3(
            mode="analyze",
            s3_location="s3://bucket/path",
            database_name="db",
            table_name="t",
        )
        _assert_success(result, "glue_s3")
        assert "s3_location" in result
        assert "optimization_recommendations" in result or "analysis_summary" in result


class TestGlueDataRouter:
    def test_glue_incremental(self):
        result = glue_data(
            mode="incremental",
            source_database="db",
            source_table="src",
            target_database="db",
            target_table="tgt",
            incremental_column="ts",
        )
        _assert_success(result, "glue_data incremental")
        template = result.get("job_template") or result.get("template") or ""
        assert str(template).strip()
        assert result.get("incremental_column") == "ts"

    def test_glue_incremental_requires_params(self):
        result = glue_data(mode="incremental")
        _assert_error(result, name="glue_data missing params")

    def test_glue_bookmarks(self):
        result = glue_data(mode="bookmarks", job_name="demo-job")
        _assert_success(result, "glue_data bookmarks")


class TestRefactorRouter:
    def test_refactor_utilities(self):
        result = refactor(
            mode="utilities",
            code_samples=[
                "df.filter(col('a') == 1)",
                "df.filter(col('b') == 2)",
            ],
        )
        _assert_success(result, "refactor")
        assert "utilities_module" in result or "functions" in result

    def test_refactor_pipeline_project(self):
        result = refactor(
            mode="pipeline",
            sql_content="SELECT id FROM users",
            workspace_name="demo-project",
        )
        assert isinstance(result, dict)
        assert result.get("status") in {"success", "info"}


class TestSearchRouter:
    def test_search_conversions(self):
        result = search(mode="conversions", query="SELECT")
        _assert_success(result, "search")
        assert "results" in result
        assert isinstance(result["results"], list)


class TestContextRouter:
    def test_context_store_requires_args(self):
        result = context(mode="store")
        _assert_error(
            result, message_contains="conversion_id", name="context store missing"
        )

    def test_context_store_and_get(self):
        stored = context(
            mode="store", conversion_id="round2-k1", context_data={"a": 1}
        )
        _assert_success(stored, "context store")
        got = context(mode="get", conversion_id="round2-k1")
        _assert_success(got, "context get")
        assert got.get("key") == "round2-k1"
        assert got.get("value") == {"a": 1}

    def test_context_assist(self):
        result = context(mode="assist", sql_query="SELECT id FROM users")
        assert result.get("status") in {"active", "success", "ready"}
        assert isinstance(result, dict)


class TestBatchStatusRouter:
    def test_batch_active(self):
        result = batch_status(mode="active")
        _assert_success(result, "batch_status active")
        jobs = result.get("jobs")
        assert isinstance(jobs, (list, dict)), f"jobs not list/dict: {type(jobs)}"
        assert "active_job_count" in result or isinstance(jobs, list)

    def test_batch_recent(self):
        result = batch_status(mode="recent", limit=5, status="completed")
        _assert_success(result, "batch_status recent")


class TestS3SourceRouter:
    def test_s3_analyze(self):
        result = s3_source(mode="analyze", s3_path="s3://bucket/path")
        _assert_success(result, "s3_source")
        assert "data_source" in result or "recommendations" in result


class TestAnalyticsRouter:
    def test_analytics_optimization(self):
        result = analytics(mode="optimization", optimization_type="all")
        _assert_success(result, "analytics optimization")
        assert "metrics" in result or "effectiveness" in result

    def test_analytics_usage(self):
        result = analytics(mode="usage")
        _assert_success(result, "analytics usage")


class TestRouterSmoke:
    """Call every registered router with documented primary-mode params."""

    def test_all_fourteen_tools_succeed(self):
        calls = [
            (
                "convert",
                convert,
                {
                    "mode": "sql",
                    "sql_query": "SELECT * FROM users",
                    "dialect": "postgres",
                },
            ),
            ("analyze", analyze, {"mode": "sql_context", "sql_content": "SELECT 1"}),
            ("optimize", optimize, {"mode": "code", "code": "df.select('a')"}),
            ("review", review, {"mode": "code", "code": "df.show()"}),
            (
                "glue_job",
                glue_job,
                {
                    "mode": "template",
                    "job_name": "smoke",
                    "sql_query": "SELECT * FROM users",
                },
            ),
            (
                "glue_schema",
                glue_schema,
                {
                    "mode": "detect",
                    "sample_data": [{"id": 1}],
                    "table_name": "t",
                },
            ),
            (
                "glue_s3",
                glue_s3,
                {
                    "mode": "analyze",
                    "s3_location": "s3://b/p",
                    "database_name": "db",
                    "table_name": "t",
                },
            ),
            (
                "glue_data",
                glue_data,
                {
                    "mode": "incremental",
                    "source_database": "db",
                    "source_table": "src",
                    "target_database": "db",
                    "target_table": "tgt",
                    "incremental_column": "ts",
                },
            ),
            (
                "refactor",
                refactor,
                {
                    "mode": "utilities",
                    "code_samples": ["df.select('a')", "df.select('b')"],
                },
            ),
            ("search", search, {"mode": "conversions", "query": "SELECT"}),
            (
                "context",
                context,
                {
                    "mode": "store",
                    "conversion_id": "smoke-r2",
                    "context_data": {"n": 1},
                },
            ),
            ("batch_status", batch_status, {"mode": "active"}),
            ("s3_source", s3_source, {"mode": "analyze", "s3_path": "s3://b/p"}),
            ("analytics", analytics, {"mode": "optimization", "optimization_type": "all"}),
        ]
        assert len(calls) == 14
        for name, fn, kwargs in calls:
            result = fn(**kwargs)
            _assert_success(result, name)
            if name == "convert":
                ast.parse(result["pyspark_code"])
