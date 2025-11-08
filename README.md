# PySpark Tools - FastMCP Server

A FastMCP server for intelligent SQL to PySpark conversion with batch processing, code review, and AWS Glue optimization.

## Features

- **SQL to PySpark Conversion** - Convert SQL queries to optimized PySpark code
- **Batch Processing** - Process multiple SQL files and PDFs concurrently  
- **Code Review** - Analyze PySpark code for best practices and AWS Glue compatibility
- **PDF Extraction** - Extract SQL queries from PDF documents
- **Memory Management** - Persistent storage for conversions and optimization history

## Project Structure

```
pyspark_tools/
├── pyspark_tools/          # Core package
│   ├── server.py          # FastMCP server
│   ├── sql_converter.py   # SQL to PySpark conversion
│   ├── batch_processor.py # Batch processing
│   ├── code_reviewer.py   # Code review and optimization
│   ├── memory_manager.py  # Data persistence
│   └── file_utils.py      # File handling utilities
├── tests/                 # Test suite
├── docs/                  # Detailed documentation
├── docker-compose.yml     # Container orchestration
└── requirements.txt       # Dependencies
```

## Quick Start

### Docker (Recommended)

```bash
# Start the server
docker-compose up -d

# Run tests
make test-all

# View logs
docker-compose logs -f pyspark-tools
```

### Local Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python run_server.py
```

## MCP Tools

The server provides 39 MCP tools for SQL conversion, batch processing, and code optimization:

- **convert_sql_to_pyspark** - Convert SQL to PySpark code
- **batch_process_files** - Process multiple files concurrently
- **review_pyspark_code** - Code review and best practices
- **generate_aws_glue_job_template** - AWS Glue job generation
- **extract_sql_from_pdf** - PDF SQL extraction

See [docs/api.md](docs/api.md) for complete tool reference.

## Usage

```bash
# MCP integration
uvx fastmcp serve pyspark_tools.server:app

# Batch processing
make batch-process DIR=/path/to/sql/files

# Run tests
make test-all
```

## Testing

```bash
# Run all tests
make test-all

# Run specific module tests
make test-sql-converter
make test-batch-processor

# Run with coverage
make coverage-html
```

See [docs/testing.md](docs/testing.md) for detailed testing guide.

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Format and lint
make format
make lint

# Run tests
make test-all
```

See [docs/development.md](docs/development.md) for detailed development guide.

## Documentation

- [Installation Guide](docs/installation.md)
- [Usage Guide](docs/usage.md) 
- [API Reference](docs/api.md)
- [Testing Guide](docs/testing.md)
- [Development Guide](docs/development.md)

## License

MIT License - see [LICENSE](LICENSE) file for details.
