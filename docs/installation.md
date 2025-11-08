# Installation Guide

## Prerequisites

- Python 3.10+
- Docker (recommended)
- Git

## Docker Installation (Recommended)

### Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd pyspark_tools

# Start the server
docker-compose up -d

# Verify installation
docker-compose logs pyspark-tools
```

### Docker Services

The docker-compose.yml defines several services:

- **pyspark-tools**: Main server service
- **pyspark-tools-test**: Test runner service
- **test-***: Individual module test services

### Volume Mounts

- `./data` - Persistent database and memory storage
- `./input` - Input SQL files and PDFs for processing
- `./output` - Generated PySpark code and batch results
- `./cache` - Documentation and pattern cache

## Local Installation

### Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Run the Server

```bash
# Start the FastMCP server
python run_server.py

# Or with uvx for MCP integration
uvx fastmcp serve pyspark_tools.server:app
```

## MCP Integration

### With Kiro

Add to your MCP configuration:

```json
{
  "mcpServers": {
    "pyspark-tools": {
      "command": "uvx",
      "args": ["fastmcp", "serve", "pyspark_tools.server:app"],
      "env": {
        "FASTMCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### Environment Variables

- `FASTMCP_LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `DATABASE_PATH`: Custom SQLite database path
- `MAX_WORKERS`: Maximum concurrent workers for batch processing
- `OUTPUT_DIR`: Default output directory for generated files

## Verification

### Test Installation

```bash
# Run basic tests
make test-quick

# Run full test suite
make test-all

# Check server health
curl http://localhost:8000/health
```

### Common Issues

#### Docker Issues

```bash
# If Docker daemon not running
sudo systemctl start docker

# If permission issues
sudo usermod -aG docker $USER
# Then logout and login again
```

#### Python Version Issues

```bash
# Check Python version
python --version

# If using older Python, install Python 3.10+
# On Ubuntu/Debian:
sudo apt update
sudo apt install python3.10 python3.10-venv

# On macOS with Homebrew:
brew install python@3.10
```

#### Dependency Issues

```bash
# Clear pip cache
pip cache purge

# Reinstall dependencies
pip install --force-reinstall -r requirements.txt

# If SQLGlot issues
pip install --upgrade sqlglot
```

## Next Steps

- [Usage Guide](usage.md) - Learn how to use the tools
- [API Reference](api.md) - Complete MCP tool reference
- [Development Guide](development.md) - Set up development environment