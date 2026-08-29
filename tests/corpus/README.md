# SQL conversion corpus

Warehouse-shaped queries used as a syntax gate in CI.

- Always: `convert(mode=sql)` returns `status=success` and `ast.parse(pyspark_code)`.
- Optional semantic compare: `pip install 'pyspark-tools[spark]'` then run
  `pytest tests/test_sql_corpus.py::test_corpus_spark_round_trip_row_counts`.
  Cases with a `views` map register temp views and compare row counts/hashes
  against `spark.sql(sqlglot.transpile(...))`.

CI without the `[spark]` extra stays green (that test skips).

Oracle `NVL(...)` currently hangs inside SQLGlot's oracle parser on this
package's converter path; the corpus uses `COALESCE` (the mapped form) plus
`DECODE` for Oracle null/branch handling. Do not add raw `NVL(` cases until
that hang is fixed upstream or locally.
