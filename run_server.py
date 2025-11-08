#!/usr/bin/env python3
"""Startup script for the PySpark Tools MCP server."""

from pyspark_tools.server import app

if __name__ == "__main__":
    # Run the server using stdio transport (synchronous)
    app.run(transport="stdio")
