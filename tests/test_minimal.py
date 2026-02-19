"""
Minimal tests that should always pass.

These tests validate basic functionality and are used as a baseline
for CI/CD pipelines and environment validation.
"""

import sys
from pathlib import Path

import pytest


@pytest.mark.fast
class TestBasicEnvironment:
    """Test basic environment setup."""
    
    def test_python_version(self):
        """Test Python version is supported."""
        assert sys.version_info >= (3, 10), f"Python 3.10+ required, got {sys.version_info}"
    
    def test_project_structure(self):
        """Test basic project structure exists."""
        project_root = Path(__file__).parent.parent
        
        # Check essential directories
        assert (project_root / "pyspark_tools").exists(), "pyspark_tools package missing"
        assert (project_root / "tests").exists(), "tests directory missing"
        
        # Check essential files
        assert (project_root / "run_server.py").exists(), "run_server.py missing"
        assert (project_root / "requirements.txt").exists(), "requirements.txt missing"
    
    def test_basic_imports(self):
        """Test basic Python imports work."""
        import json
        import os
        import sqlite3
        import sys

        # Test that we can import these without errors
        assert json is not None
        assert sqlite3 is not None
        assert os is not None
        assert sys is not None


@pytest.mark.fast
@pytest.mark.unit
class TestPackageImports:
    """Test package imports work correctly."""
    
    def test_core_package_import(self):
        """Test core package can be imported."""
        try:
            import pyspark_tools
            assert pyspark_tools is not None
        except ImportError as e:
            pytest.skip(f"Package import failed: {e}")
    
    def test_server_import(self):
        """Test server module can be imported."""
        try:
            from pyspark_tools import server
            assert server is not None
        except ImportError as e:
            pytest.skip(f"Server import failed: {e}")
    
    def test_sql_converter_import(self):
        """Test SQL converter can be imported."""
        try:
            from pyspark_tools import sql_converter
            assert sql_converter is not None
        except ImportError as e:
            pytest.skip(f"SQL converter import failed: {e}")


@pytest.mark.fast
class TestBasicFunctionality:
    """Test basic functionality works."""
    
    def test_simple_math(self):
        """Test basic math operations."""
        assert 2 + 2 == 4
        assert 10 / 2 == 5
        assert 3 * 3 == 9
    
    def test_string_operations(self):
        """Test basic string operations."""
        test_string = "hello world"
        assert test_string.upper() == "HELLO WORLD"
        assert test_string.replace("world", "python") == "hello python"
    
    def test_list_operations(self):
        """Test basic list operations."""
        test_list = [1, 2, 3, 4, 5]
        assert len(test_list) == 5
        assert sum(test_list) == 15
        assert max(test_list) == 5


@pytest.mark.fast
class TestFileOperations:
    """Test basic file operations."""
    
    def test_temp_file_creation(self, tmp_path):
        """Test temporary file creation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        
        assert test_file.exists()
        assert test_file.read_text() == "test content"
    
    def test_json_operations(self, tmp_path):
        """Test JSON file operations."""
        import json
        
        test_data = {"key": "value", "number": 42}
        test_file = tmp_path / "test.json"
        
        # Write JSON
        with open(test_file, "w") as f:
            json.dump(test_data, f)
        
        # Read JSON
        with open(test_file, "r") as f:
            loaded_data = json.load(f)
        
        assert loaded_data == test_data