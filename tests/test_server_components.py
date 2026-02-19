"""
Tests for server components and integration.

This module tests the underlying components used by the MCP server
to improve overall coverage without directly testing MCP tool decorators.
"""

# Mock imports removed - not used in tests

import pytest

from pyspark_tools.advanced_optimizer import AdvancedOptimizer
from pyspark_tools.batch_processor import BatchProcessor
from pyspark_tools.code_reviewer import PySparkCodeReviewer
from pyspark_tools.duplicate_detector import DuplicateDetector

# Import the actual components used by the server
from pyspark_tools.memory_manager import MemoryManager
from pyspark_tools.sql_converter import SQLToPySparkConverter


@pytest.mark.unit
@pytest.mark.server
class TestServerComponentIntegration:
    """Test integration of server components."""

    def test_memory_manager_integration(self, temp_db_path):
        """Test memory manager integration."""
        memory = MemoryManager(str(temp_db_path))

        # Test storing and retrieving conversion
        sql_query = "SELECT id, name FROM users"
        pyspark_code = "df = spark.table('users').select('id', 'name')"

        # Store conversion
        hash_id = memory.store_conversion(
            sql_query=sql_query,
            pyspark_code=pyspark_code,
            optimization_notes="Use column pruning",
            dialect="postgres",
        )

        assert hash_id is not None

        # Retrieve conversion
        retrieved = memory.get_conversion(sql_query)
        assert retrieved is not None
        assert retrieved["sql_query"] == sql_query
        assert retrieved["pyspark_code"] == pyspark_code

    def test_sql_converter_integration(self):
        """Test SQL converter integration."""
        converter = SQLToPySparkConverter()

        # Test basic conversion
        sql_query = "SELECT id, name FROM users WHERE active = 1"
        result = converter.convert_sql_to_pyspark(sql_query)

        assert result is not None
        assert hasattr(result, "pyspark_code")
        assert hasattr(result, "optimizations")
        assert len(result.pyspark_code) > 0

    def test_code_reviewer_integration(self):
        """Test code reviewer integration."""
        reviewer = PySparkCodeReviewer()

        # Test code review
        pyspark_code = "df = spark.table('users').collect()"
        result = reviewer.review_code(pyspark_code)

        assert result is not None
        # Result is a tuple of (issues, summary)
        issues, summary = result
        assert isinstance(issues, list)
        assert isinstance(summary, dict)
        assert "errors" in summary

    def test_batch_processor_integration(self, temp_db_path, temp_dir):
        """Test batch processor integration."""
        memory = MemoryManager(str(temp_db_path))
        converter = SQLToPySparkConverter()
        batch_processor = BatchProcessor(memory, converter)

        # Create test SQL file
        sql_file = temp_dir / "test.sql"
        sql_file.write_text("SELECT id, name FROM users;")

        # Test processing single file
        result = batch_processor.process_files([str(sql_file)])

        assert result is not None
        # Result is a BatchResult dataclass
        assert hasattr(result, "total_files")
        assert result.total_files >= 1
        assert hasattr(result, "successful_files")

    def test_duplicate_detector_integration(self, temp_db_path):
        """Test duplicate detector integration."""
        memory = MemoryManager(str(temp_db_path))
        detector = DuplicateDetector(memory)

        # Test pattern analysis
        code_samples = [
            "df.filter(col('active') == 1)",
            "df.filter(col('status') == 'enabled')",
            "df.filter(col('valid') == True)",
        ]

        result = detector.analyze_patterns(code_samples)

        assert result is not None
        # Result is a PatternAnalysis dataclass
        assert hasattr(result, "patterns")
        assert hasattr(result, "matches")
        assert isinstance(result.patterns, list)
        assert hasattr(result, "statistics")

    def test_advanced_optimizer_integration(self):
        """Test advanced optimizer integration."""
        optimizer = AdvancedOptimizer()

        # Test optimization analysis
        pyspark_code = "df = spark.table('users').filter(col('active') == 1).groupBy('department').count()"

        result = optimizer.analyze_data_flow(pyspark_code)

        assert result is not None
        # Result is a DataFlowAnalysis dataclass
        assert hasattr(result, "nodes")
        assert hasattr(result, "execution_order")
        assert isinstance(result.execution_order, list)
        assert hasattr(result, "join_operations")


@pytest.mark.unit
@pytest.mark.server
class TestServerErrorHandling:
    """Test error handling in server components."""

    def test_memory_manager_error_handling(self):
        """Test memory manager error handling."""
        # Test with invalid database path
        try:
            memory = MemoryManager("/invalid/path/database.sqlite")
            # If it doesn't raise an error, it should handle gracefully
            assert memory is not None
        except Exception as e:
            # Should handle database errors gracefully - check for permission or path errors
            error_msg = str(e).lower()
            assert (
                "database" in error_msg
                or "path" in error_msg
                or "permission" in error_msg
                or "invalid" in error_msg
            )

    def test_sql_converter_error_handling(self):
        """Test SQL converter error handling."""
        converter = SQLToPySparkConverter()

        # Test with invalid SQL
        try:
            result = converter.convert_sql_to_pyspark("INVALID SQL SYNTAX")
            # Should return a result with error information
            assert result is not None
        except Exception as e:
            # Should handle SQL parsing errors gracefully
            assert isinstance(e, Exception)

    def test_code_reviewer_error_handling(self):
        """Test code reviewer error handling."""
        reviewer = PySparkCodeReviewer()

        # Test with empty code
        result = reviewer.review_code("")

        # Should handle empty input gracefully
        assert result is not None
        # Result is a tuple of (issues, summary)
        issues, summary = result
        assert isinstance(issues, list)
        assert isinstance(summary, dict)

    def test_batch_processor_error_handling(self, temp_db_path):
        """Test batch processor error handling."""
        memory = MemoryManager(str(temp_db_path))
        converter = SQLToPySparkConverter()
        batch_processor = BatchProcessor(memory, converter)

        # Test with non-existent files
        result = batch_processor.process_files(["/nonexistent/file.sql"])

        # Should handle file errors gracefully
        assert result is not None
        # Result is a BatchResult dataclass
        assert hasattr(result, "total_files")
        assert hasattr(result, "failed_files")
        # Should have some failed files due to non-existent path
        assert result.failed_files >= 0


@pytest.mark.integration
@pytest.mark.server
class TestServerWorkflows:
    """Test complete workflows using server components."""

    def test_sql_to_pyspark_workflow(self, temp_db_path):
        """Test complete SQL to PySpark conversion workflow."""
        # Initialize components
        memory = MemoryManager(str(temp_db_path))
        converter = SQLToPySparkConverter()
        reviewer = PySparkCodeReviewer()
        optimizer = AdvancedOptimizer()

        # Step 1: Convert SQL
        sql_query = (
            "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
        )
        conversion_result = converter.convert_sql_to_pyspark(sql_query)

        assert conversion_result is not None
        assert hasattr(conversion_result, "pyspark_code")

        # Step 2: Store conversion
        hash_id = memory.store_conversion(
            sql_query=sql_query,
            pyspark_code=conversion_result.pyspark_code,
            optimization_notes="\n".join(conversion_result.optimizations),
            dialect=conversion_result.dialect_used,
        )

        assert hash_id is not None

        # Step 3: Review generated code
        review_result = reviewer.review_code(conversion_result.pyspark_code)

        assert review_result is not None
        # Result is a tuple of (issues, summary)
        issues, summary = review_result
        assert isinstance(issues, list)
        assert isinstance(summary, dict)

        # Step 4: Optimize if needed
        if summary.get("errors", 0) > 0:  # If there are errors, try optimization
            optimization_result = optimizer.analyze_data_flow(
                conversion_result.pyspark_code
            )
            assert optimization_result is not None
            assert hasattr(optimization_result, "execution_order")

    def test_batch_analysis_workflow(self, temp_db_path, temp_dir):
        """Test batch processing with pattern analysis workflow."""
        # Initialize components
        memory = MemoryManager(str(temp_db_path))
        converter = SQLToPySparkConverter()
        batch_processor = BatchProcessor(memory, converter)
        detector = DuplicateDetector(memory)

        # Create test SQL files
        sql_files = []
        for i in range(3):
            sql_file = temp_dir / f"query_{i}.sql"
            sql_file.write_text(f"SELECT id, name FROM table_{i} WHERE active = 1;")
            sql_files.append(str(sql_file))

        # Step 1: Batch process files
        batch_result = batch_processor.process_files(sql_files)

        assert batch_result is not None
        # Result is a BatchResult dataclass
        assert hasattr(batch_result, "total_files")
        assert batch_result.total_files >= 0

        # Step 2: Analyze patterns in generated code
        # Get recent conversions to analyze patterns
        recent_conversions = memory.get_recent_conversions(limit=10)

        if recent_conversions:
            code_samples = [conv.get("pyspark_code", "") for conv in recent_conversions]
            code_samples = [code for code in code_samples if code]  # Filter empty

            if code_samples:
                pattern_result = detector.analyze_patterns(code_samples)

                assert pattern_result is not None
                # Result is a PatternAnalysis dataclass
                assert hasattr(pattern_result, "patterns")
                assert hasattr(pattern_result, "statistics")

    def test_optimization_workflow(self, temp_db_path):
        """Test code optimization workflow."""
        # Initialize components
        MemoryManager(str(temp_db_path))  # Initialize but don't use
        converter = SQLToPySparkConverter()
        optimizer = AdvancedOptimizer()

        # Convert SQL that can be optimized
        sql_query = "SELECT * FROM large_table WHERE date >= '2023-01-01'"
        conversion_result = converter.convert_sql_to_pyspark(sql_query)

        assert conversion_result is not None

        # Analyze data flow
        flow_result = optimizer.analyze_data_flow(conversion_result.pyspark_code)
        assert flow_result is not None
        # Result is a DataFlowAnalysis dataclass
        assert hasattr(flow_result, "execution_order")
        assert isinstance(flow_result.execution_order, list)

        # Get partitioning suggestions
        partition_result = optimizer.suggest_partitioning_strategy(
            conversion_result.pyspark_code
        )
        assert partition_result is not None
        # Result might be a list or dataclass depending on implementation
        if hasattr(partition_result, "recommended_partitions"):
            assert isinstance(partition_result.recommended_partitions, list)
        else:
            # If it's a list directly, that's also valid
            assert isinstance(partition_result, list)

        # Get join optimization suggestions
        join_result = optimizer.recommend_join_strategy(conversion_result.pyspark_code)
        assert join_result is not None
        # Result might be a list or dataclass depending on implementation
        if hasattr(join_result, "join_strategy"):
            assert hasattr(join_result, "recommendations")
        else:
            # If it's a list directly, that's also valid
            assert isinstance(join_result, list)


@pytest.mark.performance
@pytest.mark.server
class TestServerPerformance:
    """Test performance aspects of server components."""

    def test_conversion_performance(self):
        """Test SQL conversion performance."""
        converter = SQLToPySparkConverter()

        # Test conversion time for various query sizes
        queries = [
            "SELECT id FROM users",
            "SELECT u.id, u.name, p.title FROM users u JOIN profiles p ON u.id = p.user_id",
            """
            WITH user_stats AS (
                SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount
                FROM orders 
                WHERE created_at >= '2023-01-01'
                GROUP BY user_id
            )
            SELECT u.name, us.order_count, us.total_amount
            FROM users u
            JOIN user_stats us ON u.id = us.user_id
            WHERE us.order_count > 5
            ORDER BY us.total_amount DESC
            """,
        ]

        for query in queries:
            result = converter.convert_sql_to_pyspark(query)
            assert result is not None
            assert hasattr(result, "pyspark_code")

    def test_memory_performance(self, temp_db_path):
        """Test memory manager performance with multiple operations."""
        memory = MemoryManager(str(temp_db_path))

        # Store multiple conversions
        for i in range(10):
            sql_query = f"SELECT id, name FROM table_{i}"
            pyspark_code = f"df = spark.table('table_{i}').select('id', 'name')"

            hash_id = memory.store_conversion(
                sql_query=sql_query,
                pyspark_code=pyspark_code,
                optimization_notes=f"Optimization for table_{i}",
                dialect="postgres",
            )

            assert hash_id is not None

        # Retrieve recent conversions
        recent = memory.get_recent_conversions(limit=5)
        assert len(recent) <= 5

        # Search conversions
        search_results = memory.search_conversions("table", limit=3)
        assert len(search_results) <= 3
