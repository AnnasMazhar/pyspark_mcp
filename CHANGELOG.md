# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
