"""Template corpus regression tests.

Extracts SQL embedded in aws_glue_integration.py and README.md, then adds
representative dialect/join/cast patterns. Each case must either convert
successfully to parseable Python or return a clear error — never status=success
with mangled placeholders or a cartesian `True` join predicate.
"""

import ast
import re
from pathlib import Path

import pytest

from pyspark_tools.server import _convert_sql_to_pyspark_internal

REPO = Path(__file__).resolve().parents[1]
JOIN_TRUE_RE = re.compile(r"\.join\([^,]+,\s*True\s*,")
PLACEHOLDER_MANGLE_RE = re.compile(r":schema\.|::bit\(")


def _looks_like_sql(sql: str) -> bool:
    """Keep real SQL; drop Glue Python templates that merely mention SELECT."""
    text = sql.strip()
    if re.search(r"\b(def |class |import |from awsglue)", text):
        return False
    head = re.sub(r"^--[^\n]*\n", "", text).strip()
    return bool(re.match(r"(WITH|SELECT)\b", head, re.IGNORECASE))


def _extract_select_sql(text: str):
    """Pull SQL-ish SELECT strings from source/docs (not Python control flow)."""
    found = []
    # Triple-quoted / fenced blocks containing SELECT
    for block in re.findall(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE):
        if re.search(r"\bSELECT\b", block, re.IGNORECASE):
            found.append(block.strip())
    for block in re.findall(r'"""(.*?)"""', text, re.DOTALL):
        if re.search(r"\bSELECT\b", block, re.IGNORECASE) and "def " not in block:
            # Keep only lines that look like SQL, not Glue Python templates
            if "from awsglue" in block or "spark.sql" in block and "import" in block:
                # Extract inner spark.sql SELECT literals
                for inner in re.findall(
                    r"""spark\.sql\((?:f?["']{1,3})(.*?)(?:["']{1,3})\)""",
                    block,
                    re.DOTALL,
                ):
                    if re.search(r"\bSELECT\b", inner, re.IGNORECASE):
                        found.append(inner.strip())
            elif re.match(r"^\s*SELECT\b", block.strip(), re.IGNORECASE):
                found.append(block.strip())
    # Single-line SELECT ... in string literals
    for stmt in re.findall(
        r"""(?<![A-Za-z_])(?:f?["'])([^"']*\bSELECT\b[^"']*)["']""",
        text,
        re.IGNORECASE,
    ):
        stmt = stmt.strip()
        if stmt.upper().startswith("SELECT") or "SELECT " in stmt.upper():
            # Drop comments-only prefixes
            sql = re.sub(r"^--[^\n]*\n", "", stmt).strip()
            if re.search(r"\bSELECT\b", sql, re.IGNORECASE):
                found.append(sql)
    # Dedup preserving order
    seen = set()
    out = []
    for s in found:
        key = re.sub(r"\s+", " ", s)
        if key not in seen and len(s) < 2000:
            seen.add(key)
            out.append(s)
    return out


def _extracted_queries():
    glue = (REPO / "pyspark_tools" / "aws_glue_integration.py").read_text()
    readme = (REPO / "README.md").read_text()
    queries = []
    for src, text in (("glue", glue), ("readme", readme)):
        for i, sql in enumerate(_extract_select_sql(text)):
            queries.append((f"{src}_{i}", sql))
    return queries


REPRESENTATIVE = [
    ("schema_placeholder", "SELECT {schema}.id, {schema}.name FROM {schema}.users"),
    (
        "bit_cast",
        "SELECT {schema}.id::bit(32)::int AS id FROM {schema}.users",
    ),
    ("text_cast", "SELECT name::TEXT FROM users"),
    ("or_join", "SELECT * FROM a JOIN b ON a.id = b.id OR a.id2 = b.id2"),
    (
        "and_join",
        "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id AND c.active = 1",
    ),
    ("inner_join", "SELECT * FROM a JOIN b ON a.id = b.id"),
    ("left_join", "SELECT * FROM a LEFT JOIN b ON a.id = b.id"),
    (
        "three_table",
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.b_id",
    ),
    (
        "group_by",
        "SELECT a.id, SUM(b.amt) FROM a JOIN b ON a.id = b.a_id GROUP BY a.id",
    ),
    (
        "window",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t",
    ),
    (
        "cte",
        "WITH x AS (SELECT id FROM users WHERE active = 1) SELECT * FROM x",
    ),
    ("is_not_null", "SELECT * FROM t WHERE a IS NOT NULL AND b IS NULL"),
    ("between", "SELECT * FROM t WHERE ts BETWEEN 1 AND 10"),
    ("in_list", "SELECT * FROM t WHERE id IN (1, 2, 3)"),
    (
        "readme_example",
        "SELECT o.customer_id, c.name, SUM(o.amount) AS total "
        "FROM orders o JOIN customers c ON o.customer_id = c.id "
        "WHERE o.status = 'paid' GROUP BY o.customer_id, c.name",
    ),
    ("simple_select", "SELECT id FROM users"),
    (
        "analyze_join",
        "SELECT * FROM orders o JOIN items i ON o.id = i.order_id",
    ),
    ("glue_source", "SELECT * FROM source_table"),
    (
        "glue_incremental_monitor",
        "SELECT COUNT(*) as processed_records, MAX(last_modified) as last_processed FROM target_table",
    ),
    (
        "glue_bookmark",
        "SELECT * FROM information_schema.job_bookmarks WHERE job_name = 'demo'",
    ),
    (
        "glue_watermark",
        "SELECT max_value FROM {watermark_table} WHERE job_name = '{job_name}'",
    ),
    (
        "self_join",
        "SELECT * FROM a t1 JOIN a t2 ON t1.id = t2.parent_id",
    ),
    (
        "database_table_placeholder",
        "SELECT * FROM {database}.{table_name} WHERE id > 1",
    ),
]


def _all_cases():
    cases = list(REPRESENTATIVE)
    # Merge extracted queries that aren't already represented
    existing = {re.sub(r"\s+", " ", sql) for _, sql in cases}
    for name, sql in _extracted_queries():
        if not _looks_like_sql(sql):
            continue
        key = re.sub(r"\s+", " ", sql)
        if key not in existing:
            cases.append((name, sql))
            existing.add(key)
    return cases


CORPUS = _all_cases()


@pytest.mark.parametrize("name,sql", CORPUS, ids=[c[0] for c in CORPUS])
def test_template_corpus_case(name, sql):
    assert len(CORPUS) >= 20, f"corpus too small: {len(CORPUS)}"
    result = _convert_sql_to_pyspark_internal(sql, dialect="postgres", store_result=False)
    assert isinstance(result, dict)
    status = result.get("status")
    assert status in {"success", "error"}, f"{name}: bad status {status!r}"

    if status == "error":
        message = str(result.get("message") or "") + " ".join(
            result.get("warnings") or []
        )
        assert message.strip(), f"{name}: error with empty message"
        return

    code = result.get("pyspark_code") or ""
    assert code.strip(), f"{name}: success with empty pyspark_code"
    ast.parse(code)
    assert not JOIN_TRUE_RE.search(code), (
        f"{name}: cartesian True join predicate\n{code}"
    )
    assert not PLACEHOLDER_MANGLE_RE.search(code), (
        f"{name}: placeholder/cast mangling survived\n{code}"
    )


def test_corpus_has_required_patterns():
    blobs = " ".join(sql for _, sql in CORPUS).upper()
    assert "{SCHEMA}" in blobs or "{schema}" in " ".join(sql for _, sql in CORPUS)
    assert "BIT(32)" in blobs
    assert " JOIN " in blobs and " OR " in blobs
    assert "LEFT JOIN" in blobs
    assert "GROUP BY" in blobs
    assert "OVER" in blobs or "ROW_NUMBER" in blobs
    assert "WITH " in blobs
    assert "IS NOT NULL" in blobs
    assert "BETWEEN" in blobs
    assert " IN (" in blobs
    assert len(CORPUS) >= 20
