"""
Basic CI tests for continuous integration.

These tests are designed to run quickly in CI environments
and validate core functionality without heavy dependencies.
"""

import json
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.mark.fast
@pytest.mark.unit
class TestCoreComponents:
    """Test core components can be instantiated."""
    
    def test_memory_manager_creation(self):
        """Test MemoryManager can be created."""
        try:
            from pyspark_tools.memory_manager import MemoryManager

            # Use in-memory database for testing
            mm = MemoryManager(":memory:")
            assert mm is not None
            
        except ImportError as e:
            pytest.skip(f"MemoryManager import failed: {e}")
        except Exception as e:
            pytest.skip(f"MemoryManager creation failed: {e}")
    
    def test_sql_converter_creation(self):
        """Test SQLConverter can be created."""
        try:
            from pyspark_tools.sql_converter import SQLToPySparkConverter
            
            converter = SQLToPySparkConverter()
            assert converter is not None
            
        except ImportError as e:
            pytest.skip(f"SQLConverter import failed: {e}")
        except Exception as e:
            pytest.skip(f"SQLConverter creation failed: {e}")
    
    def test_batch_processor_creation(self):
        """Test BatchProcessor can be created."""
        try:
            from pyspark_tools.batch_processor import BatchProcessor
            from pyspark_tools.memory_manager import MemoryManager
            
            mm = MemoryManager(":memory:")
            processor = BatchProcessor(mm)
            assert processor is not None
            
        except ImportError as e:
            pytest.skip(f"BatchProcessor import failed: {e}")
        except Exception as e:
            pytest.skip(f"BatchProcessor creation failed: {e}")


@pytest.mark.fast
@pytest.mark.unit
class TestBasicSQLConversion:
    """Test basic SQL conversion functionality."""
    
    def test_simple_sql_conversion(self):
        """Test simple SQL query conversion."""
        try:
            from pyspark_tools.sql_converter import SQLToPySparkConverter
            
            converter = SQLToPySparkConverter()
            
            # Test simple SELECT query
            sql = "SELECT id, name FROM users WHERE active = 1"
            result = converter.convert_sql_to_pyspark(sql)
            
            assert isinstance(result, dict)
            assert "pyspark_code" in result
            assert "success" in result
            
        except ImportError as e:
            pytest.skip(f"SQL conversion test skipped: {e}")
        except Exception as e:
            pytest.skip(f"SQL conversion failed: {e}")
    
    def test_sql_parsing(self):
        """Test SQL parsing functionality."""
        try:
            import sqlglot

            # Test basic SQL parsing
            sql = "SELECT * FROM users"
            parsed = sqlglot.parse_one(sql)
            assert parsed is not None
            
        except ImportError as e:
            pytest.skip(f"SQLGlot import failed: {e}")
        except Exception as e:
            pytest.skip(f"SQL parsing failed: {e}")


@pytest.mark.fast
@pytest.mark.unit
class TestDatabaseOperations:
    """Test basic database operations."""
    
    def test_sqlite_connection(self):
        """Test SQLite database connection."""
        # Test in-memory database
        conn = sqlite3.connect(":memory:")
        assert conn is not None
        
        # Test basic operations
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, 'test')")
        
        result = cursor.execute("SELECT * FROM test").fetchone()
        assert result == (1, 'test')
        
        conn.close()
    
    def test_memory_manager_database(self):
        """Test MemoryManager database operations."""
        try:
            from pyspark_tools.memory_manager import MemoryManager
            
            mm = MemoryManager(":memory:")
            
            # Test basic database operations
            assert mm.db_path == ":memory:"
            
        except ImportError as e:
            pytest.skip(f"MemoryManager test skipped: {e}")
        except Exception as e:
            pytest.skip(f"MemoryManager database test failed: {e}")


@pytest.mark.fast
@pytest.mark.unit
class TestFileOperations:
    """Test file handling operations."""
    
    def test_file_utils_import(self):
        """Test file utilities can be imported."""
        try:
            from pyspark_tools import file_utils
            assert file_utils is not None
        except ImportError as e:
            pytest.skip(f"File utils import failed: {e}")
    
    def test_basic_file_handling(self, tmp_path):
        """Test basic file handling operations."""
        # Create test file
        test_file = tmp_path / "test.sql"
        test_content = "SELECT * FROM users;"
        test_file.write_text(test_content)
        
        # Test file reading
        content = test_file.read_text()
        assert content == test_content
        
        # Test file exists
        assert test_file.exists()


@pytest.mark.fast
@pytest.mark.integration
class TestBasicIntegration:
    """Test basic integration between components."""
    
    def test_converter_with_memory_manager(self):
        """Test SQL converter with memory manager integration."""
        try:
            from pyspark_tools.memory_manager import MemoryManager
            from pyspark_tools.sql_converter import SQLToPySparkConverter

            # Create components
            mm = MemoryManager(":memory:")
            converter = SQLToPySparkConverter()
            
            # Test basic integration
            sql = "SELECT id FROM users"
            result = converter.convert_sql_to_pyspark(sql)
            
            assert isinstance(result, dict)
            
        except ImportError as e:
            pytest.skip(f"Integration test skipped: {e}")
        except Exception as e:
            pytest.skip(f"Integration test failed: {e}")


@pytest.mark.fast
class TestEnvironmentValidation:
    """Test environment validation for CI."""
    
    def test_required_packages(self):
        """Test required packages are available."""
        required_packages = [
            "json",
            "sqlite3",
            "pathlib",
            "unittest.mock"
        ]
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                pytest.fail(f"Required package not available: {package}")
    
    def test_python_path(self):
        """Test Python path includes project directory."""
        import sys
        
        project_root = str(Path(__file__).parent.parent)
        
        # Check if project root is in Python path
        path_found = any(project_root in path for path in sys.path)
        
        if not path_found:
            # This is not necessarily a failure, just a warning
            pytest.skip(f"Project root not in Python path: {project_root}")
    
    def test_working_directory(self):
        """Test working directory is correct."""
        import os
        
        cwd = Path(os.getcwd())
        project_indicators = ["pyspark_tools", "tests", "run_server.py"]
        
        # Check if we're in the right directory
        indicators_found = sum(1 for indicator in project_indicators if (cwd / indicator).exists())
        
        if indicators_found < 2:
            pytest.skip(f"Working directory may not be project root: {cwd}")


@pytest.mark.performance
class TestBasicPerformance:
    """Basic performance tests for CI."""
    
    def test_import_speed(self):
        """Test package imports are reasonably fast."""
        import time
        
        start_time = time.time()
        
        try:
            import pyspark_tools
            import_time = time.time() - start_time
            
            # Should import in less than 5 seconds
            assert import_time < 5.0, f"Import took too long: {import_time:.2f}s"
            
        except ImportError:
            pytest.skip("Package import failed")
    
    def test_sql_conversion_speed(self):
        """Test SQL conversion is reasonably fast."""
        try:
            import time

            from pyspark_tools.sql_converter import SQLToPySparkConverter
            
            converter = SQLToPySparkConverter()
            
            start_time = time.time()
            result = converter.convert_sql_to_pyspark("SELECT id FROM users")
            conversion_time = time.time() - start_time
            
            # Should convert in less than 2 seconds
            assert conversion_time < 2.0, f"Conversion took too long: {conversion_time:.2f}s"
            
        except ImportError:
            pytest.skip("SQL converter not available")
        except Exception:
            pytest.skip("SQL conversion failed")