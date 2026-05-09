"""Tests for consolidated tool router functions."""
import pytest
from pyspark_tools.consolidated_tools import (
    convert, analyze, optimize, review, glue_job, glue_schema,
    glue_s3, glue_data, refactor, search, context, batch_status,
    s3_source, analytics
)


class TestConvertRouter:
    def test_convert_sql_mode(self):
        result = convert(mode="sql", sql_query="SELECT * FROM users", dialect="spark")
        assert "status" in result or "pyspark_code" in result or "error" in result

    def test_convert_invalid_mode(self):
        result = convert(mode="invalid_mode")
        assert "error" in result or "Unknown mode" in str(result)


class TestAnalyzeRouter:
    def test_analyze_sql_context(self):
        result = analyze(mode="sql_context")
        assert result is not None

    def test_analyze_invalid_mode(self):
        result = analyze(mode="nope")
        assert "error" in result or "Unknown" in str(result)


class TestOptimizeRouter:
    def test_optimize_code(self):
        result = optimize(mode="code", code="df.filter(col('x') == 1).select('y')")
        assert result is not None


class TestReviewRouter:
    def test_review_code(self):
        result = review(mode="code", code="df = spark.table('t')\ndf.show()")
        assert result is not None


class TestGlueJobRouter:
    def test_glue_template(self):
        result = glue_job(mode="properties")
        assert result is not None


class TestGlueSchemaRouter:
    def test_glue_detect(self):
        result = glue_schema(mode="detect")
        assert result is not None


class TestGlueS3Router:
    def test_glue_s3_analyze(self):
        result = glue_s3(mode="analyze")
        assert result is not None


class TestGlueDataRouter:
    def test_glue_incremental(self):
        result = glue_data(mode="incremental")
        assert result is not None


class TestSearchRouter:
    def test_search_conversions(self):
        result = search(mode="conversions")
        assert result is not None


class TestContextRouter:
    def test_context_store(self):
        result = context(mode="store")
        assert result is not None

    def test_context_get(self):
        result = context(mode="get")
        assert result is not None


class TestBatchStatusRouter:
    def test_batch_active(self):
        result = batch_status(mode="active")
        assert result is not None


class TestS3SourceRouter:
    def test_s3_analyze(self):
        result = s3_source(mode="analyze")
        assert result is not None


class TestAnalyticsRouter:
    def test_analytics_optimization(self):
        result = analytics(mode="optimization")
        assert result is not None
