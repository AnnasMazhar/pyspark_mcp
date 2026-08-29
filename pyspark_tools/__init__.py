"""PySpark Tools - FastMCP server for SQL to PySpark conversion and optimization."""

from __future__ import annotations

import sys

__version__ = "0.0.7"

_DEPRECATED_CLI_WARNING = (
    "this is pyspark-tools (SQL→PySpark codegen), not "
    "SemyonSinchenko/pyspark-mcp (live Spark)"
)


def main():
    """CLI entry point for the pyspark-tools MCP server."""
    from pyspark_tools.server import app

    try:
        app.run()
    except KeyboardInterrupt:
        pass


def deprecated_mcp_main():
    """Deprecated ``pyspark-mcp`` console script: warn, then run the server."""
    sys.stderr.write(_DEPRECATED_CLI_WARNING + "\n")
    main()
