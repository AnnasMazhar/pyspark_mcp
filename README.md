# PySpark Tools MCP Server

Convert SQL to optimized PySpark code with multi-dialect support, AWS Glue integration, and intelligent optimization recommendations.

[![CI Pipeline](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/AnnasMazhar/pyspark_mcp/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Quick Start

```bash
# Install
pip install -e .

# Run server
python run_server.py

# Run tests
pytest tests/ -v
```

## Features

- **49 MCP Tools** - Complete SQL to PySpark conversion toolkit
- **Multi-Dialect Support** - 31 SQL dialects via SQLGlot
- **AWS Glue Integration** - DynamicFrame, job templates, Data Catalog
- **Batch Processing** - 100+ concurrent file processing
- **Pattern Detection** - Code deduplication and optimization
- **71% Test Coverage** - 256 tests passing

## Documentation

- [Installation Guide](docs/installation.md)
- [Usage Examples](docs/usage.md)
- [API Reference](docs/api.md)
- [Testing Guide](TESTING_GUIDE.md)
- [CI/CD Documentation](docs/ci/)
- [Optimization Guide](docs/OPTIMIZATION_ANALYSIS.md)

## Development

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

# Test
pytest tests/ -v --cov=pyspark_tools

# Format
black pyspark_tools tests
isort pyspark_tools tests

# Lint
flake8 pyspark_tools tests
```

## CI/CD Status

- ✅ All tests passing (256/256)
- ✅ 71% code coverage
- ✅ Code quality checks
- ✅ Python 3.11 tested

## License

MIT License - see [LICENSE](LICENSE) file for details.
