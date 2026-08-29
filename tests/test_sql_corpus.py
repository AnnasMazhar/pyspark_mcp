"""P1-1: golden SQL corpus — syntax gate always, Spark round-trip optional.

Each case in tests/corpus/ must convert with status=success and ast.parse.
When the [spark] extra is installed, cases that declare views are executed
against temp views and compared to spark.sql(sqlglot.transpile(...)).
CI without pyspark stays green (semantic tests skip).
"""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORPUS_DIR = ROOT / "tests" / "corpus"
CASES_PATH = CORPUS_DIR / "cases.json"

REQUIRED_TAGS = {
    "oracle_decode",
    "oracle_nvl",
    "postgres_cast",
    "redshift_dateadd",
    "window",
    "cte",
    "union",
    "fromless",
    "placeholder",
}


def _load_cases() -> list[dict]:
    assert CASES_PATH.is_file(), (
        f"missing corpus manifest {CASES_PATH} — add tests/corpus/ with ≥20 cases"
    )
    cases = json.loads(CASES_PATH.read_text())
    assert isinstance(cases, list) and len(cases) >= 20, (
        f"corpus must have ≥20 cases, got {len(cases) if isinstance(cases, list) else type(cases)}"
    )
    return cases


def _case_sql(case: dict) -> str:
    if "sql" in case and case["sql"].strip():
        return case["sql"].strip()
    sql_file = CORPUS_DIR / case["file"]
    return sql_file.read_text().strip()


def _case_ids():
    return [c["id"] for c in _load_cases()]


class TestCorpusManifest:
    def test_corpus_covers_required_ugly_shapes(self):
        cases = _load_cases()
        tags = {t for c in cases for t in c.get("tags", [])}
        missing = REQUIRED_TAGS - tags
        assert not missing, f"corpus missing required tags: {sorted(missing)}"

    def test_corpus_sql_is_not_toy_select_star(self):
        cases = _load_cases()
        toys = []
        for case in cases:
            sql = " ".join(_case_sql(case).split())
            if sql.upper() in {"SELECT * FROM USERS", "SELECT * FROM USERS;"}:
                toys.append(case["id"])
        assert not toys, f"toy queries are not a corpus: {toys}"


@pytest.mark.parametrize("case_id", _case_ids() if CASES_PATH.is_file() else ["missing"])
def test_corpus_case_succeeds_and_parses(case_id):
    from pyspark_tools.consolidated_tools import convert

    cases = {c["id"]: c for c in _load_cases()}
    case = cases[case_id]
    sql = _case_sql(case)
    result = convert(mode="sql", sql_query=sql, dialect=case.get("dialect"))
    assert result.get("status") == "success", (
        f"{case_id}: expected success, got {result.get('status')!r} "
        f"message={result.get('message')!r} warnings={result.get('warnings')!r}"
    )
    code = result["pyspark_code"]
    ast.parse(code)
    assert "from pyspark.sql.functions import *" not in code


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _spark_available(), reason="optional [spark] extra not installed")
def test_corpus_spark_round_trip_row_counts():
    """Semantic compare: generated DF vs spark.sql(transpile). Optional extra."""
    import sqlglot
    from pyspark.sql import SparkSession

    from pyspark_tools.sql_converter import SQLToPySparkConverter

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pyspark-tools-corpus")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    converter = SQLToPySparkConverter()
    mismatches = []
    ran = 0
    try:
        for case in _load_cases():
            views = case.get("views")
            if not views:
                continue
            ran += 1
            sql = _case_sql(case)
            dialect = case.get("dialect") or "spark"
            for name, rows in views.items():
                spark.createDataFrame(rows).createOrReplaceTempView(name)
            result = converter.convert_sql_to_pyspark(sql, dialect=dialect)
            assert result.success, case["id"]
            ns = {"spark": spark}
            exec(result.pyspark_code, ns)  # noqa: S102 — generated corpus code
            generated = ns["result_df"]
            transpiled = sqlglot.transpile(sql, read=dialect, write="spark")[0]
            expected = spark.sql(transpiled)
            g_count, e_count = generated.count(), expected.count()
            g_hash = _row_hash(generated)
            e_hash = _row_hash(expected)
            if g_count != e_count or g_hash != e_hash:
                mismatches.append(
                    f"{case['id']}: gen={g_count}/{g_hash} sql={e_count}/{e_hash}"
                )
        assert ran >= 1, "at least one corpus case must declare views for spark compare"
        assert mismatches == [], "spark round-trip mismatches: " + "; ".join(mismatches)
    finally:
        spark.stop()


def _row_hash(df) -> str:
    rows = [tuple(r) for r in df.collect()]
    blob = repr(sorted(rows, key=repr)).encode()
    return hashlib.sha256(blob).hexdigest()[:16]
