"""
Pytest configuration and fixtures for PySpark Tools testing.

This module provides shared fixtures and configuration for all test modules.
"""

import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import Mock, patch

import pytest

from pyspark_tools.batch_processor import (
    BatchProcessor,
    BatchProgress,
    BatchResult,
    BatchStatus,
    ConversionResult,
)
from pyspark_tools.duplicate_detector import (
    CodePattern,
    DuplicateDetector,
    PatternAnalysis,
    PatternMatch,
)
from pyspark_tools.file_utils import (
    ExtractedSQL,
    FileHandler,
    FileProcessingResult,
    OutputManager,
    SQLQueryExtractor,
)
from pyspark_tools.memory_manager import (
    BatchJob,
    DuplicatePattern,
    MemoryManager,
    PerformanceMetric,
)

# Import modules under test
from pyspark_tools.sql_converter import SQLToPySparkConverter


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Create a temporary directory for test data."""
    temp_dir = Path(tempfile.mkdtemp(prefix="pyspark_tools_test_"))
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def sample_sql_files(test_data_dir: Path) -> Dict[str, Path]:
    """Create sample SQL files for testing."""
    sql_files = {}

    # Simple SELECT query
    simple_sql = test_data_dir / "simple_query.sql"
    simple_sql.write_text(
        """
    SELECT id, name, email
    FROM users
    WHERE active = 1
    ORDER BY name;
    """
    )
    sql_files["simple"] = simple_sql

    # Complex query with JOINs
    complex_sql = test_data_dir / "complex_query.sql"
    complex_sql.write_text(
        """
    SELECT u.id, u.name, p.title, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN profiles p ON u.id = p.user_id
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = 1 AND p.created_at > '2023-01-01'
    GROUP BY u.id, u.name, p.title
    HAVING COUNT(o.id) > 5
    ORDER BY order_count DESC;
    """
    )
    sql_files["complex"] = complex_sql

    # PostgreSQL specific query
    postgres_sql = test_data_dir / "postgres_query.sql"
    postgres_sql.write_text(
        """
    SELECT 
        string_agg(name, ', ') as names,
        array_agg(id) as ids,
        extract(year from created_at) as year
    FROM users
    WHERE name ~ '^[A-Z]'
    GROUP BY extract(year from created_at);
    """
    )
    sql_files["postgres"] = postgres_sql

    # Oracle specific query
    oracle_sql = test_data_dir / "oracle_query.sql"
    oracle_sql.write_text(
        """
    SELECT 
        id,
        name,
        nvl(email, 'no-email') as email,
        decode(status, 1, 'Active', 0, 'Inactive', 'Unknown') as status_text,
        rownum as row_number
    FROM users
    WHERE rownum <= 100;
    """
    )
    sql_files["oracle"] = oracle_sql

    return sql_files


@pytest.fixture(scope="session")
def sample_pdf_files(test_data_dir: Path) -> Dict[str, Path]:
    """Create sample PDF files for testing (mock content)."""
    pdf_files = {}

    # Note: These are placeholder files for testing file handling
    # Real PDF content would require more complex setup
    simple_pdf = test_data_dir / "simple_report.pdf"
    simple_pdf.write_bytes(
        b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
    )
    pdf_files["simple"] = simple_pdf

    return pdf_files


@pytest.fixture
def temp_db_path(test_data_dir: Path) -> Generator[Path, None, None]:
    """Provide a temporary database path for testing."""
    db_path = test_data_dir / f"test_{os.getpid()}.sqlite"
    yield db_path
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def sql_converter() -> SQLToPySparkConverter:
    """Provide a SQL converter instance for testing."""
    return SQLToPySparkConverter()


@pytest.fixture
def memory_manager(temp_db_path: Path) -> MemoryManager:
    """Provide a memory manager instance with temporary database."""
    return MemoryManager(str(temp_db_path))


@pytest.fixture
def batch_processor(memory_manager: MemoryManager) -> BatchProcessor:
    """Provide a batch processor instance for testing."""
    return BatchProcessor(memory_manager)


@pytest.fixture
def duplicate_detector(memory_manager: MemoryManager) -> DuplicateDetector:
    """Provide a duplicate detector instance for testing."""
    return DuplicateDetector(memory_manager)


@pytest.fixture
def sql_extractor() -> SQLQueryExtractor:
    """Provide a SQL extractor instance for testing."""
    return SQLQueryExtractor()


@pytest.fixture
def file_handler() -> FileHandler:
    """Provide a file handler instance for testing."""
    return FileHandler()


@pytest.fixture
def output_manager() -> OutputManager:
    """Provide an output manager instance for testing."""
    return OutputManager()


@pytest.fixture
def mock_spark_session():
    """Provide a mock Spark session for testing."""
    with patch("pyspark.sql.SparkSession") as mock_spark:
        mock_session = Mock()
        mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session
        yield mock_session


@pytest.fixture
def sample_pyspark_code() -> str:
    """Provide sample PySpark code for testing."""
    return """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('TestApp').getOrCreate()

# Load data
df = spark.table('users')

# Transform data
result = df.filter(col('active') == 1) \\
           .select('id', 'name', 'email') \\
           .orderBy('name')

result.show()
"""


@pytest.fixture
def sample_duplicate_code() -> Dict[str, str]:
    """Provide sample code with duplicates for testing."""
    return {
        "file1.py": """
def process_data(df):
    return df.filter(col('active') == 1).select('id', 'name')

def main():
    df = spark.table('users')
    result = process_data(df)
    return result
""",
        "file2.py": """
def handle_users(dataframe):
    return dataframe.filter(col('active') == 1).select('id', 'name')

def run():
    user_df = spark.table('users')
    processed = handle_users(user_df)
    return processed
""",
    }


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("PYSPARK_TOOLS_TEST_MODE", "1")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).parent.parent))


@pytest.fixture
def capture_logs(caplog):
    """Capture logs for testing."""
    import logging

    caplog.set_level(logging.INFO)
    return caplog


# Performance testing fixtures
@pytest.fixture
def performance_sql_queries() -> Dict[str, str]:
    """Provide SQL queries for performance testing."""
    return {
        "simple": "SELECT * FROM users WHERE id = 1",
        "medium": """
            SELECT u.*, p.title 
            FROM users u 
            JOIN profiles p ON u.id = p.user_id 
            WHERE u.active = 1
        """,
        "complex": """
            WITH user_stats AS (
                SELECT user_id, COUNT(*) as order_count
                FROM orders
                GROUP BY user_id
            )
            SELECT u.name, us.order_count, p.title
            FROM users u
            JOIN user_stats us ON u.id = us.user_id
            JOIN profiles p ON u.id = p.user_id
            WHERE us.order_count > 10
            ORDER BY us.order_count DESC
        """,
    }


# Integration testing fixtures
@pytest.fixture
def integration_test_data(test_data_dir: Path) -> Dict[str, Any]:
    """Provide data for integration testing."""
    # Create a more complex directory structure
    input_dir = test_data_dir / "input"
    output_dir = test_data_dir / "output"
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    # Create multiple SQL files
    for i in range(5):
        sql_file = input_dir / f"query_{i}.sql"
        sql_file.write_text(f"SELECT * FROM table_{i} WHERE id > {i * 10};")

    return {"input_dir": input_dir, "output_dir": output_dir, "file_count": 5}


# Module-specific markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "sql_converter: SQL converter module tests")
    config.addinivalue_line("markers", "batch_processor: Batch processor module tests")
    config.addinivalue_line(
        "markers", "duplicate_detector: Duplicate detector module tests"
    )
    config.addinivalue_line("markers", "file_utils: File utilities module tests")
    config.addinivalue_line("markers", "server: MCP server module tests")
    config.addinivalue_line("markers", "memory_manager: Memory manager module tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "performance: Performance tests")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file names."""
    for item in items:
        # Add markers based on test file names
        if "test_sql_converter" in item.fspath.basename:
            item.add_marker(pytest.mark.sql_converter)
        elif "test_batch_processor" in item.fspath.basename:
            item.add_marker(pytest.mark.batch_processor)
        elif "test_duplicate_detector" in item.fspath.basename:
            item.add_marker(pytest.mark.duplicate_detector)
        elif "test_file_utils" in item.fspath.basename:
            item.add_marker(pytest.mark.file_utils)
        elif "test_server" in item.fspath.basename:
            item.add_marker(pytest.mark.server)
        elif "test_memory_manager" in item.fspath.basename:
            item.add_marker(pytest.mark.memory_manager)

        # Add integration marker for integration tests
        if "integration" in item.name.lower():
            item.add_marker(pytest.mark.integration)

        # Add performance marker for performance tests
        if "performance" in item.name.lower():
            item.add_marker(pytest.mark.performance)
            item.add_marker(pytest.mark.slow)
