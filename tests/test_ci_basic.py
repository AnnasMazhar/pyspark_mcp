"""
Basic CI tests for continuous integration.

These tests run quickly in CI and validate core functionality.
Failures are reported; they are not skipped.
"""

import sqlite3
from pathlib import Path

import pytest


@pytest.mark.fast
@pytest.mark.unit
class TestCoreComponents:
    """Test core components can be instantiated."""

    def test_memory_manager_creation(self):
        """Test MemoryManager can be created."""
        from pyspark_tools.memory_manager import MemoryManager

        mm = MemoryManager(":memory:")
        assert mm is not None

    def test_sql_converter_creation(self):
        """Test SQLConverter can be created."""
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        converter = SQLToPySparkConverter()
        assert converter is not None

    def test_batch_processor_creation(self):
        """Test BatchProcessor can be created."""
        from pyspark_tools.batch_processor import BatchProcessor
        from pyspark_tools.memory_manager import MemoryManager

        mm = MemoryManager(":memory:")
        processor = BatchProcessor(mm)
        assert processor is not None


@pytest.mark.fast
@pytest.mark.unit
class TestBasicSQLConversion:
    """Test basic SQL conversion functionality."""

    def test_simple_sql_conversion(self):
        """Test simple SQL query conversion."""
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        converter = SQLToPySparkConverter()
        sql = "SELECT id, name FROM users WHERE active = 1"
        result = converter.convert_sql_to_pyspark(sql)

        assert isinstance(result, dict)
        assert "pyspark_code" in result
        assert "success" in result
        assert result["success"] is True
        assert result["pyspark_code"]

    def test_sql_parsing(self):
        """Test SQL parsing functionality."""
        import sqlglot

        sql = "SELECT * FROM users"
        parsed = sqlglot.parse_one(sql)
        assert parsed is not None


@pytest.mark.fast
@pytest.mark.unit
class TestDatabaseOperations:
    """Test basic database operations."""

    def test_sqlite_connection(self):
        """Test SQLite database connection."""
        conn = sqlite3.connect(":memory:")
        assert conn is not None

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, 'test')")

        result = cursor.execute("SELECT * FROM test").fetchone()
        assert result == (1, "test")

        conn.close()

    def test_memory_manager_database(self):
        """Test MemoryManager database operations."""
        from pyspark_tools.memory_manager import MemoryManager

        mm = MemoryManager(":memory:")
        assert mm.db_path == Path(":memory:") or str(mm.db_path).endswith(":memory:")


@pytest.mark.fast
@pytest.mark.unit
class TestFileOperations:
    """Test file handling operations."""

    def test_file_utils_import(self):
        """Test file utilities can be imported."""
        from pyspark_tools import file_utils

        assert file_utils is not None

    def test_basic_file_handling(self, tmp_path):
        """Test basic file handling operations."""
        test_file = tmp_path / "test.sql"
        test_content = "SELECT * FROM users;"
        test_file.write_text(test_content)

        content = test_file.read_text()
        assert content == test_content
        assert test_file.exists()


@pytest.mark.fast
@pytest.mark.integration
class TestBasicIntegration:
    """Test basic integration between components."""

    def test_converter_with_memory_manager(self):
        """Test SQL converter with memory manager integration."""
        from pyspark_tools.memory_manager import MemoryManager
        from pyspark_tools.sql_converter import SQLToPySparkConverter

        mm = MemoryManager(":memory:")
        converter = SQLToPySparkConverter()
        assert mm is not None

        sql = "SELECT id FROM users"
        result = converter.convert_sql_to_pyspark(sql)

        assert isinstance(result, dict)
        assert result.get("success") is True


@pytest.mark.fast
class TestEnvironmentValidation:
    """Test environment validation for CI."""

    def test_required_packages(self):
        """Test required packages are available."""
        required_packages = ["json", "sqlite3", "pathlib", "unittest.mock"]

        for package in required_packages:
            __import__(package)

    def test_python_path(self):
        """Test Python path includes project directory."""
        import sys

        project_root = str(Path(__file__).parent.parent)
        path_found = any(project_root in path for path in sys.path)
        assert path_found or (Path.cwd() / "pyspark_tools").exists()

    def test_working_directory(self):
        """Test working directory is the project root (or contains it)."""
        import os

        cwd = Path(os.getcwd())
        project_indicators = ["pyspark_tools", "tests", "run_server.py"]
        indicators_found = sum(
            1 for indicator in project_indicators if (cwd / indicator).exists()
        )
        assert indicators_found >= 2, f"Working directory may not be project root: {cwd}"


@pytest.mark.performance
class TestBasicPerformance:
    """Basic performance tests for CI."""

    def test_import_speed(self):
        """Test package imports are reasonably fast."""
        import time

        start_time = time.time()
        import pyspark_tools  # noqa: F401

        import_time = time.time() - start_time
        assert import_time < 5.0, f"Import took too long: {import_time:.2f}s"

    def test_sql_conversion_speed(self):
        """Test SQL conversion is reasonably fast."""
        import time

        from pyspark_tools.sql_converter import SQLToPySparkConverter

        converter = SQLToPySparkConverter()
        start_time = time.time()
        result = converter.convert_sql_to_pyspark("SELECT id FROM users")
        conversion_time = time.time() - start_time

        assert result["success"] is True
        assert conversion_time < 2.0, f"Conversion took too long: {conversion_time:.2f}s"
