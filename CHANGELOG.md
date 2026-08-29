# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Console script is `pyspark-tools`; `pyspark-mcp` is a deprecated alias that
  warns this is SQL→PySpark codegen, not SemyonSinchenko/pyspark-mcp.
- Conversion cache keys include dialect (and table_info) so postgres/spark
  requests never share an entry.
- Default Glue job version is 5.0; `job.commit()` runs on success only.
- Converter default emit is production-shaped (no `import *`, no `show()`).

### Fixed
- FROM-less SQL (`SELECT 1`) now emits `spark.sql(...)` that `ast.parse`s.
- `{foo.bar}` placeholders substitute to identifier paths; leftover braces
  return `status=error` naming them.
- Glue templates: one correct DynamicFrame import; bookmark warning when
  the body uses `spark.sql` / DataFrame reads.
- `optimize` returns `original_code` + `suggestions` (does not claim a rewrite).
- `glue_s3` analysis is labeled path-heuristic; invented `% faster` strings removed.

## [0.0.5] - 2026-08-29

### Fixed
- Template `{schema}` placeholders no longer mangle Postgres casts into
  `col(':schema.id')` / leftover `::bit(` fragments.
- JOIN ON ... OR ... no longer emits a cartesian `True` join predicate.
- `__version__` / PyPI metadata published as 0.0.5 (audit-fix release).

## [0.0.4] - 2026-06-08

### Changed
- Consolidated the MCP surface from 51 individual tools to 14 `mode=` routers
  (`convert`, `analyze`, `optimize`, `review`, `glue_job`, `glue_schema`,
  `glue_s3`, `glue_data`, `refactor`, `search`, `context`, `batch_status`,
  `s3_source`, `analytics`). Helpers in `server.py` remain callable directly.
- Dependabot bumps: `actions/checkout` 4 → 6, `actions/github-script` 7 → 9,
  `boto3` / `botocore` lower bounds.

### Fixed
- PyPI package description now ships via the `readme` field in `pyproject.toml`.

## [0.0.3] - 2026-05-09

### Fixed
- Duplicate `[project.optional-dependencies]` section in `pyproject.toml` that
  broke installs.
- README `mcp-name` text required by the MCP registry validator.

## [0.0.2] - 2026-05-09

### Added
- `mcp-name` metadata for MCP registry validation.

### Changed
- Version bump for a PyPI re-publish after the registry metadata fix.

## [0.0.1] - 2026-05-09

### Added
- Initial public release: SQLGlot-based SQL → PySpark DataFrame-API source
  generation, AWS Glue job *template* strings, FastMCP server entry point
  `pyspark-mcp`, Docker/Make packaging, and a pytest suite.

### Notes
- This package generates source text. It does not run Spark, talk to AWS Glue,
  or execute SQL.
- FastMCP 3.x is the runtime (not 0.3.0). Core install does not require a JVM.
