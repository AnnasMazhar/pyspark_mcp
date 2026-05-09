"""Pytest configuration and fixtures for PySpark Tools testing."""

import os
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import Mock, patch

import pytest

from pyspark_tools.sql_converter import SQLToPySparkConverter
from pyspark_tools.memory_manager import MemoryManager
from pyspark_tools.batch_processor import BatchProcessor
from pyspark_tools.duplicate_detector import DuplicateDetector
from pyspark_tools.file_utils import FileHandler, OutputManager, SQLQueryExtractor


@pytest.fixture
def sql_converter() -> SQLToPySparkConverter:
    return SQLToPySparkConverter()


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.close()
    return db_path


@pytest.fixture
def memory_manager(temp_db_path: Path) -> Generator[MemoryManager, None, None]:
    manager = MemoryManager(str(temp_db_path))
    yield manager
    if hasattr(manager, "_conn") and manager._conn:
        try:
            manager._conn.close()
        except Exception:
            pass


@pytest.fixture
def batch_processor(memory_manager: MemoryManager) -> BatchProcessor:
    return BatchProcessor(memory_manager)


@pytest.fixture
def duplicate_detector(memory_manager: MemoryManager) -> DuplicateDetector:
    return DuplicateDetector(memory_manager)


@pytest.fixture
def sql_extractor() -> SQLQueryExtractor:
    return SQLQueryExtractor()


@pytest.fixture
def file_handler() -> FileHandler:
    return FileHandler()


@pytest.fixture
def output_manager() -> OutputManager:
    return OutputManager()


@pytest.fixture(scope="session")
def test_data_dir() -> Generator[Path, None, None]:
    temp_path = Path(tempfile.mkdtemp(prefix="pyspark_tools_test_"))
    yield temp_path
    import shutil
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="session")
def sample_sql_files(test_data_dir: Path) -> Dict[str, Path]:
    sql_files = {}
    (test_data_dir / "simple.sql").write_text("SELECT id, name FROM users WHERE active = 1;")
    sql_files["simple"] = test_data_dir / "simple.sql"
    (test_data_dir / "complex.sql").write_text("""
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1 GROUP BY u.id, u.name HAVING COUNT(o.id) > 5;
    """)
    sql_files["complex"] = test_data_dir / "complex.sql"
    return sql_files


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    monkeypatch.setenv("PYSPARK_TOOLS_TEST_MODE", "1")
    yield


@pytest.fixture
def performance_sql_queries() -> Dict[str, str]:
    return {
        "simple": "SELECT * FROM users WHERE id = 1",
        "medium": "SELECT u.*, p.title FROM users u JOIN profiles p ON u.id = p.user_id WHERE u.active = 1",
        "complex": """
            WITH user_stats AS (
                SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id
            )
            SELECT u.name, us.order_count FROM users u
            JOIN user_stats us ON u.id = us.user_id WHERE us.order_count > 10
        """,
    }


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for tests."""
    return tmp_path


@pytest.fixture
def optimized_memory_manager(temp_db_path: Path) -> MemoryManager:
    """Provide a memory manager instance."""
    return MemoryManager(str(temp_db_path))


@pytest.fixture
def cached_mock_data() -> Dict[str, Any]:
    """Provide mock data for tests."""
    return {
        "users": [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "active": 1},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "active": 1},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "active": 0},
        ],
        "profiles": [{"user_id": 1, "title": "Engineer"}, {"user_id": 2, "title": "Manager"}],
        "orders": [
            {"id": 1, "user_id": 1, "amount": 100.0},
            {"id": 2, "user_id": 1, "amount": 150.0},
            {"id": 3, "user_id": 2, "amount": 200.0},
        ],
    }


@pytest.fixture
def monitor_test_performance():
    """Provide test performance monitoring (no-op for CI)."""
    yield
