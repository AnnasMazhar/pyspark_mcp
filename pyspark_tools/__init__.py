"""PySpark Tools - FastMCP server for SQL to PySpark conversion and optimization."""

__version__ = "0.1.0"

def main():
    """CLI entry point for pyspark-mcp server."""
    from pyspark_tools.server import app
    try:
        app.run()
    except KeyboardInterrupt:
        pass
