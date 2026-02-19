#!/usr/bin/env python3
"""Integration tests to verify the MCP server is working properly."""

import pytest


def test_server_import():
    """Test that we can import the server module."""
    from pyspark_tools.server import app

    assert app is not None


def test_server_tools():
    """Test that server tools are properly registered."""
    from pyspark_tools import server

    # Check for key functions in the module
    expected_functions = [
        "convert_sql_to_pyspark",
        "analyze_data_flow",
        "suggest_partitioning_strategy",
        "recommend_join_strategy",
        "generate_comprehensive_optimizations",
    ]

    found_functions = []
    for func_name in expected_functions:
        if hasattr(server, func_name):
            found_functions.append(func_name)

    # At least 3 key functions should be available
    assert len(found_functions) >= 3, f"Missing key functions. Found: {found_functions}"


def test_sql_conversion():
    """Test basic SQL to PySpark conversion."""
    from pyspark_tools.sql_converter import SQLToPySparkConverter

    converter = SQLToPySparkConverter()
    test_sql = (
        "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id"
    )

    result = converter.convert_sql_to_pyspark(test_sql)

    assert result is not None
    assert hasattr(result, "pyspark_code")
    assert result.pyspark_code
    assert hasattr(result, "dialect_used")
    assert hasattr(result, "optimizations")


def test_optimization_engine():
    """Test the advanced optimization engine."""
    from pyspark_tools.advanced_optimizer import AdvancedOptimizer

    optimizer = AdvancedOptimizer()
    test_code = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

customers_df = spark.table('customers')
orders_df = spark.table('orders')
result_df = customers_df.join(orders_df, 'customer_id')
result_df.show()
"""

    analysis = optimizer.analyze_data_flow(test_code)

    assert analysis is not None
    assert hasattr(analysis, "nodes")
    assert hasattr(analysis, "join_operations")
    assert hasattr(analysis, "estimated_total_cost")
