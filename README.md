# 🚀 PySpark Tools - FastMCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastMCP](https://img.shields.io/badge/FastMCP-compatible-green.svg)](https://github.com/jlowin/fastmcp)

A powerful FastMCP server that provides intelligent SQL to PySpark conversion, code review, and optimization capabilities with AWS Glue best practices. Built for seamless integration with AI assistants and workflow automation tools.

## ✨ Key Features

- 🔄 **SQL to PySpark Conversion**: Intelligent conversion of complex SQL queries to optimized PySpark code
- 🔍 **Code Review**: Comprehensive analysis for best practices, performance, and AWS Glue compatibility  
- 🧠 **Memory Management**: Persistent storage and retrieval of conversion history and context
- ☁️ **AWS Glue Integration**: Generate production-ready Glue job templates with best practices
- ⚡ **Performance Optimization**: Advanced suggestions for query performance and resource efficiency
- 🔗 **MCP Integration**: Seamless integration with AI assistants via Model Context Protocol
- 📊 **Analytics**: Track conversion patterns and code quality metrics

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

## Usage

### Running the Server

```bash
# Run with uvx (recommended)
uvx fastmcp serve pyspark_tools.server:app

# Or run directly
python -m pyspark_tools.server
```

### Available Tools

1. **convert_sql_to_pyspark**: Convert SQL queries to PySpark code
2. **review_pyspark_code**: Review PySpark code for issues and best practices
3. **optimize_pyspark_code**: Get optimization suggestions for PySpark code
4. **get_conversion_history**: Retrieve recent conversion history
5. **search_conversions**: Search through stored conversions
6. **store_context/get_context**: Store and retrieve context information
7. **generate_aws_glue_job_template**: Generate AWS Glue job templates

## Configuration

The server uses SQLite for memory management. The default database location is:
`/home/dev/.cache/mcp/memory.sqlite`

## Examples

### Convert SQL to PySpark

```python
# Input SQL
sql = "SELECT customer_id, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer_id"

# Converted PySpark code will include:
# - Proper imports
# - DataFrame operations
# - Optimization suggestions
```

### Review PySpark Code

```python
# The reviewer checks for:
# - AWS Glue compatibility
# - Performance issues
# - Best practices
# - Code style
```

### AWS Glue Best Practices

The server includes checks for:
- Glue-specific imports and context usage
- Job bookmarking
- Parameter handling
- Supported operations
- File format recommendations

## 🛠️ Development

```bash
# Clone the repository
git clone https://github.com/AnnasMazhar/pyspark_mcp.git
cd pyspark_mcp

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black pyspark_tools/

# Type checking
mypy pyspark_tools/
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [FastMCP](https://github.com/jlowin/fastmcp) for Model Context Protocol integration
- Powered by [PySpark](https://spark.apache.org/docs/latest/api/python/) for distributed data processing
- Optimized for [AWS Glue](https://aws.amazon.com/glue/) serverless data integration

## 📞 Support

If you have any questions or need help, please:
- Open an issue on GitHub
- Check the [documentation](README.md)
- Review existing issues and discussions

---

**Made with ❤️ for the PySpark and data engineering community**
