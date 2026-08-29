# Installation Guide

## Prerequisites

- Python 3.11+
- Git
- Docker (optional; stdio MCP only — there is no HTTP server)

## PyPI (recommended)

```bash
pip install pyspark-tools
pyspark-tools
```

Zero-clone: `uvx pyspark-tools`.

This starts a **stdio** FastMCP server. There is no `http://localhost:8000/health`
endpoint.

## Docker

The image is stdio-only. Compose is for running tests, not for an HTTP happy path.

```bash
git clone https://github.com/AnnasMazhar/pyspark_mcp.git
cd pyspark_mcp
docker compose --profile test run --rm pyspark-tools-test
```

### Docker services

The docker-compose.yml defines several services:

- **pyspark-tools**: stdio MCP process (no published HTTP port)
- **pyspark-tools-test**: Test runner service (`profiles: [test]`)
- **test-***: Individual module test services

### Volume Mounts

- `./data` - Persistent database and memory storage
- `./input` - Input SQL files and PDFs for processing
- `./output` - Generated PySpark code and batch results
- `./cache` - Documentation and pattern cache

## Local Installation (from a clone)

### Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

### Run the Server

```bash
pyspark-tools

# Development convenience (sys.path insert + banners)
python run_server.py
```

## MCP Integration

### Claude Desktop / Cursor

```json
{
  "mcpServers": {
    "pyspark-tools": {
      "command": "pyspark-tools",
      "args": []
    }
  }
}
```

### Environment Variables

- `FASTMCP_LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `PYSPARK_TOOLS_DB_PATH`: SQLite database path
- `MAX_WORKERS`: Maximum concurrent workers for batch processing
- `OUTPUT_DIR`: Default output directory for generated files

## Verification

```bash
# Run basic tests
make test-quick

# Run full test suite
make test-all

# Confirm the CLI is on PATH after install
pyspark-tools --help || true
python -c "import pyspark_tools; print(pyspark_tools.__version__)"
```

The server speaks MCP over stdio. Do not `curl` a health URL.

### Common Issues

#### Python Version Issues

```bash
python --version   # need 3.11+

# Ubuntu/Debian:
sudo apt update
sudo apt install python3.11 python3.11-venv

# macOS with Homebrew:
brew install python@3.11
```

#### Dependency Issues

```bash
pip cache purge
pip install --force-reinstall -e ".[dev]"
pip install --upgrade sqlglot
```

## Next Steps

- [Usage Guide](usage.md) - Learn how to use the tools
- [API Reference](api.md) - Complete MCP tool reference
