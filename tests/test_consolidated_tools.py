"""Tests for consolidated tool router functions."""
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


def _assert_not_kwarg_error(result):
    """Router dispatch must not TypeError on helper kwargs."""
    assert isinstance(result, dict)
    blob = json.dumps(result, default=str).lower()
    assert "unexpected keyword argument" not in blob
    assert "got an unexpected keyword" not in blob
    assert "typeerror" not in blob


class TestConvertRouter:
    def test_convert_sql_mode(self):
        result = convert(mode="sql", sql_query="SELECT * FROM users", dialect="spark")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"
        assert result.get("pyspark_code")

    def test_convert_sql_with_glue_template(self):
        result = convert(
            mode="sql",
            sql_query="SELECT id FROM users",
            include_glue_template=True,
        )
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"

    def test_convert_invalid_mode(self):
        result = convert(mode="invalid_mode")
        assert result.get("status") == "error"
        assert "Unknown mode" in result.get("message", "")


class TestAnalyzeRouter:
    def test_analyze_sql_context(self):
        result = analyze(mode="sql_context", sql_content="SELECT * FROM users")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"

    def test_analyze_workspace(self):
        result = analyze(
            mode="workspace",
            sql_content="SELECT id FROM orders",
            workspace_name="demo",
        )
        _assert_not_kwarg_error(result)
        assert result.get("status") in {"success", "info"}

    def test_analyze_invalid_mode(self):
        result = analyze(mode="nope")
        assert result.get("status") == "error"


class TestOptimizeRouter:
    def test_optimize_code(self):
        result = optimize(mode="code", code="df.filter(col('x') == 1).select('y')")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestReviewRouter:
    def test_review_code(self):
        result = review(mode="code", code="df = spark.table('t')\ndf.show()")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestGlueJobRouter:
    def test_glue_template(self):
        result = glue_job(mode="template", job_name="demo-job", sql_query="SELECT 1")
        _assert_not_kwarg_error(result)
        assert result.get("status") != "error" or "unexpected keyword" not in str(result)

    def test_glue_properties_requires_name(self):
        result = glue_job(mode="properties")
        assert result.get("status") == "error"

    def test_glue_properties(self):
        result = glue_job(mode="properties", job_name="demo-job")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestGlueSchemaRouter:
    def test_glue_detect(self):
        result = glue_schema(
            mode="detect", sample_data=[{"id": 1, "name": "a"}], table_name="users"
        )
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestGlueS3Router:
    def test_glue_s3_analyze(self):
        result = glue_s3(
            mode="analyze",
            s3_location="s3://bucket/path",
            database_name="db",
            table_name="t",
        )
        _assert_not_kwarg_error(result)


class TestGlueDataRouter:
    def test_glue_incremental_requires_params(self):
        result = glue_data(mode="incremental")
        assert result.get("status") == "error"

    def test_glue_bookmarks(self):
        result = glue_data(mode="bookmarks", job_name="demo-job")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestRefactorRouter:
    def test_refactor_utilities(self):
        result = refactor(
            mode="utilities",
            code_samples=[
                "df.filter(col('a') == 1)",
                "df.filter(col('b') == 2)",
            ],
        )
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"

    def test_refactor_pipeline_project(self):
        result = refactor(
            mode="pipeline",
            sql_content="SELECT id FROM users",
            workspace_name="demo-project",
        )
        _assert_not_kwarg_error(result)


class TestSearchRouter:
    def test_search_conversions(self):
        result = search(mode="conversions")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestContextRouter:
    def test_context_store_requires_args(self):
        result = context(mode="store")
        assert result.get("status") == "error"

    def test_context_store_and_get(self):
        stored = context(mode="store", conversion_id="k1", context_data={"a": 1})
        _assert_not_kwarg_error(stored)
        assert stored.get("status") == "success"
        got = context(mode="get", conversion_id="k1")
        _assert_not_kwarg_error(got)
        assert got.get("status") == "success"

    def test_context_assist(self):
        result = context(mode="assist", sql_query="SELECT id FROM users")
        _assert_not_kwarg_error(result)
        assert result.get("status") in {"active", "success", "ready"}


class TestBatchStatusRouter:
    def test_batch_active(self):
        result = batch_status(mode="active")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"

    def test_batch_recent(self):
        result = batch_status(mode="recent", limit=5, status="completed")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestS3SourceRouter:
    def test_s3_analyze(self):
        result = s3_source(mode="analyze", s3_path="s3://bucket/path")
        _assert_not_kwarg_error(result)


class TestAnalyticsRouter:
    def test_analytics_optimization(self):
        result = analytics(mode="optimization")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"

    def test_analytics_usage(self):
        result = analytics(mode="usage")
        _assert_not_kwarg_error(result)
        assert result.get("status") == "success"


class TestRouterSmoke:
    """Call every registered router with minimal valid input."""

    def test_all_fourteen_tools_accept_kwargs(self):
        calls = [
            ("convert", convert, {"mode": "sql", "sql_query": "SELECT 1 AS id"}),
            ("analyze", analyze, {"mode": "sql_context", "sql_content": "SELECT 1"}),
            ("optimize", optimize, {"mode": "code", "code": "df.select('a')"}),
            ("review", review, {"mode": "code", "code": "df.show()"}),
            ("glue_job", glue_job, {"mode": "properties", "job_name": "smoke"}),
            (
                "glue_schema",
                glue_schema,
                {"mode": "detect", "sample_data": [{"id": 1}], "table_name": "t"},
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
            ("glue_data", glue_data, {"mode": "bookmarks", "job_name": "smoke"}),
            (
                "refactor",
                refactor,
                {
                    "mode": "utilities",
                    "code_samples": ["df.select('a')", "df.select('b')"],
                },
            ),
            ("search", search, {"mode": "conversions"}),
            (
                "context",
                context,
                {"mode": "store", "conversion_id": "smoke", "context_data": 1},
            ),
            ("batch_status", batch_status, {"mode": "active"}),
            ("s3_source", s3_source, {"mode": "analyze", "s3_path": "s3://b/p"}),
            ("analytics", analytics, {"mode": "usage"}),
        ]
        for name, fn, kwargs in calls:
            result = fn(**kwargs)
            _assert_not_kwarg_error(result)
            assert isinstance(result, dict), f"{name} did not return a dict"
