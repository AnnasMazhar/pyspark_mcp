#!/usr/bin/env python3
"""
Entry point for the PySpark Tools MCP Server.

This script starts the FastMCP server for SQL to PySpark conversion
with 14 consolidated tools (router pattern) for LLM context efficiency.
"""

import sys
import os

# Add the pyspark_tools package to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "pyspark_tools"))

from pyspark_tools.server import app


def main():
    """Start the MCP server."""
    print("🚀 Starting PySpark Tools MCP Server...")
    print("📊 14 consolidated tools available for SQL to PySpark conversion")
    print("🔧 Server ready for connections")

    try:
        # Run the FastMCP server
        app.run()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
