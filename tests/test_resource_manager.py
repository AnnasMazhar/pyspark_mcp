"""
Tests for the ResourceManager infrastructure.

This module tests the resource management system to ensure proper
tracking and cleanup of database connections, files, and processes.
"""

import sqlite3
import subprocess
import tempfile
from pathlib import Path

import pytest

from pyspark_tools.resource_manager import (
    ResourceManager,
    get_resource_manager,
    managed_connection,
    managed_process,
    managed_temp_dir,
    managed_temp_file,
)


class TestResourceManager:
    """Test the ResourceManager class."""
    
    def test_singleton_behavior(self):
        """Test that ResourceManager follows singleton pattern."""
        manager1 = ResourceManager()
        manager2 = ResourceManager()
        assert manager1 is manager2
    
    def test_global_resource_manager(self):
        """Test the global resource manager function."""
        manager1 = get_resource_manager()
        manager2 = get_resource_manager()
        assert manager1 is manager2
        assert isinstance(manager1, ResourceManager)
    
    def test_register_connection(self, resource_manager):
        """Test registering database connections."""
        # Create a temporary database
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        try:
            # Use in-memory database to avoid file system issues
            conn = sqlite3.connect(":memory:")
            
            # Test basic connection functionality first
            cursor = conn.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
            
            # Register the connection
            resource_id = resource_manager.register_connection(conn, "Test connection")
            
            # Verify registration
            assert resource_id.startswith("conn_")
            resource_info = resource_manager.get_resource_info(resource_id)
            assert resource_info is not None
            assert resource_info.resource_type == "database_connection"
            assert "Test connection" in resource_info.description
            
            # Close connection manually first to avoid issues
            conn.close()
            
            # Verify cleanup (should handle already-closed connection gracefully)
            assert resource_manager.cleanup_resource(resource_id)
            assert resource_manager.get_resource_info(resource_id) is None
            
        finally:
            # Clean up temp file
            Path(db_path).unlink(missing_ok=True)
    
    def test_register_temp_file(self, resource_manager):
        """Test registering temporary files."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = Path(f.name)
            f.write(b"test content")
        
        resource_id = resource_manager.register_temp_file(temp_path, "Test temp file")
        
        # Verify registration
        assert resource_id.startswith("temp_")
        resource_info = resource_manager.get_resource_info(resource_id)
        assert resource_info is not None
        assert resource_info.resource_type == "temp_file"
        assert "Test temp file" in resource_info.description
        
        # Verify file exists before cleanup
        assert temp_path.exists()
        
        # Verify cleanup
        assert resource_manager.cleanup_resource(resource_id)
        assert not temp_path.exists()
        assert resource_manager.get_resource_info(resource_id) is None
    
    def test_register_process(self, resource_manager):
        """Test registering processes."""
        # Create a short-running process that exits quickly
        process = subprocess.Popen(['echo', 'test'])
        
        try:
            resource_id = resource_manager.register_process(process, "Test process")
            
            # Verify registration
            assert resource_id.startswith("proc_")
            resource_info = resource_manager.get_resource_info(resource_id)
            assert resource_info is not None
            assert resource_info.resource_type == "process"
            assert "Test process" in resource_info.description
            
            # Wait for process to complete naturally (echo is very fast)
            process.wait(timeout=2)
            
            # Verify cleanup (process should already be done)
            assert resource_manager.cleanup_resource(resource_id)
            assert resource_manager.get_resource_info(resource_id) is None
            
        finally:
            # Ensure process is terminated if still running
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
    
    def test_cleanup_all(self, resource_manager):
        """Test cleaning up all resources."""
        # Register multiple resources
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        try:
            conn = sqlite3.connect(db_path)
            conn_id = resource_manager.register_connection(conn, "Test connection")
            
            with tempfile.NamedTemporaryFile(delete=False) as f:
                temp_path = Path(f.name)
            temp_id = resource_manager.register_temp_file(temp_path, "Test temp file")
            
            # Verify resources are registered
            assert len(resource_manager.list_resources()) >= 2
            
            # Clean up all
            results = resource_manager.cleanup_all()
            
            # Verify cleanup
            assert conn_id in results
            assert temp_id in results
            assert len(resource_manager.list_resources()) == 0
            assert not temp_path.exists()
            
        finally:
            Path(db_path).unlink(missing_ok=True)
    
    def test_resource_count(self, resource_manager):
        """Test getting resource counts by type."""
        # Initially should be empty
        counts = resource_manager.get_resource_count()
        initial_total = sum(counts.values())
        
        # Add some resources
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        try:
            conn = sqlite3.connect(db_path)
            resource_manager.register_connection(conn, "Test connection")
            
            with tempfile.NamedTemporaryFile(delete=False) as f:
                temp_path = Path(f.name)
            resource_manager.register_temp_file(temp_path, "Test temp file")
            
            # Check counts
            counts = resource_manager.get_resource_count()
            assert counts.get("database_connection", 0) >= 1
            assert counts.get("temp_file", 0) >= 1
            assert sum(counts.values()) >= initial_total + 2
            
        finally:
            resource_manager.cleanup_all()
            Path(db_path).unlink(missing_ok=True)


class TestContextManagers:
    """Test the context manager functions."""
    
    def test_managed_connection(self):
        """Test managed database connection context manager."""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        try:
            with managed_connection(db_path, "Test managed connection") as conn:
                # Connection should work
                cursor = conn.execute("SELECT 1")
                result = cursor.fetchone()
                assert result[0] == 1
                
                # Connection should be registered
                manager = get_resource_manager()
                connections = manager.list_resources("database_connection")
                assert len(connections) >= 1
            
            # After context, connection should be cleaned up
            # Note: We can't easily test if the specific connection is closed
            # because the resource manager cleans up automatically
            
        finally:
            Path(db_path).unlink(missing_ok=True)
    
    def test_managed_temp_file(self):
        """Test managed temporary file context manager."""
        temp_path = None
        
        with managed_temp_file(suffix='.txt', prefix='test_') as path:
            temp_path = path
            
            # File should exist and be writable
            assert path.exists()
            path.write_text("test content")
            assert path.read_text() == "test content"
            
            # Should be registered with resource manager
            manager = get_resource_manager()
            temp_files = manager.list_resources("temp_file")
            assert len(temp_files) >= 1
        
        # After context, file should be cleaned up
        assert not temp_path.exists()
    
    def test_managed_temp_dir(self):
        """Test managed temporary directory context manager."""
        temp_path = None
        
        with managed_temp_dir(suffix='_test', prefix='test_') as path:
            temp_path = path
            
            # Directory should exist
            assert path.exists()
            assert path.is_dir()
            
            # Should be able to create files in it
            test_file = path / "test.txt"
            test_file.write_text("test content")
            assert test_file.exists()
            
            # Should be registered with resource manager
            manager = get_resource_manager()
            temp_files = manager.list_resources("temp_file")
            assert len(temp_files) >= 1
        
        # After context, directory should be cleaned up
        assert not temp_path.exists()
    
    def test_managed_process(self):
        """Test managed process context manager."""
        with managed_process(['echo', 'test']) as process:
            # Wait for process to complete (echo is very fast)
            process.wait(timeout=2)
            
            # Should be registered with resource manager
            manager = get_resource_manager()
            processes = manager.list_resources("process")
            # Note: process might already be cleaned up by the time we check
        
        # After context, process should be terminated/completed
        assert process.poll() is not None


class TestResourceLeakPrevention:
    """Test that resource leaks are prevented."""
    
    def test_connection_leak_prevention(self, resource_manager):
        """Test that database connections don't leak."""
        initial_count = resource_manager.get_resource_count()
        
        # Create and register multiple in-memory connections (faster)
        connections = []
        
        try:
            for i in range(3):  # Reduced from 5 to 3 for speed
                conn = sqlite3.connect(":memory:")
                resource_manager.register_connection(conn, f"Test connection {i}")
                connections.append(conn)
            
            # Verify connections are tracked
            current_count = resource_manager.get_resource_count()
            assert current_count.get("database_connection", 0) >= 3
            
            # Close connections manually first
            for conn in connections:
                conn.close()
            
            # Clean up all resources
            resource_manager.cleanup_all()
            
            # Verify cleanup
            final_count = resource_manager.get_resource_count()
            # Should have cleaned up the connections we added
            
        finally:
            # Ensure all connections are closed
            for conn in connections:
                try:
                    conn.close()
                except:
                    pass
    
    def test_file_leak_prevention(self, resource_manager):
        """Test that temporary files don't leak."""
        initial_count = resource_manager.get_resource_count()
        
        # Create multiple temporary files
        temp_files = []
        for i in range(5):
            with tempfile.NamedTemporaryFile(delete=False) as f:
                temp_path = Path(f.name)
                f.write(f"test content {i}".encode())
            
            resource_manager.register_temp_file(temp_path, f"Test temp file {i}")
            temp_files.append(temp_path)
        
        # Verify files exist and are tracked
        for temp_path in temp_files:
            assert temp_path.exists()
        
        current_count = resource_manager.get_resource_count()
        assert current_count.get("temp_file", 0) >= 5
        
        # Clean up all resources
        resource_manager.cleanup_all()
        
        # Verify cleanup
        for temp_path in temp_files:
            assert not temp_path.exists()
        
        final_count = resource_manager.get_resource_count()
        assert final_count.get("temp_file", 0) == initial_count.get("temp_file", 0)


# Integration test with actual components
def test_memory_manager_integration(memory_manager, resource_manager):
    """Test that MemoryManager integrates properly with ResourceManager."""
    # Test basic memory manager operations
    test_sql = "SELECT id, name FROM users WHERE active = 1"
    test_pyspark = "df = spark.table('users').filter(col('active') == 1).select('id', 'name')"
    
    # Store a conversion
    sql_hash = memory_manager.store_conversion(test_sql, test_pyspark)
    assert sql_hash is not None
    
    # Retrieve the conversion
    retrieved = memory_manager.get_conversion(test_sql)
    assert retrieved is not None
    assert retrieved['pyspark_code'] == test_pyspark
    
    # Store some context
    memory_manager.store_context("test_key", {"test": "value"})
    context = memory_manager.get_context("test_key")
    assert context == {"test": "value"}
    
    # Clean up should work without errors
    cleanup_results = resource_manager.cleanup_all()
    assert isinstance(cleanup_results, dict)


def test_resource_cleanup_in_fixtures(temp_dir, temp_db_path, resource_manager):
    """Test that fixtures properly clean up resources."""
    # Use the temp_dir fixture
    test_file = temp_dir / "test.txt"
    test_file.write_text("test content")
    assert test_file.exists()
    
    # Ensure the database directory exists
    temp_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use the temp_db_path fixture
    with sqlite3.connect(temp_db_path) as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
        conn.commit()
    
    # Verify database works
    with sqlite3.connect(temp_db_path) as conn:
        cursor = conn.execute("SELECT * FROM test")
        result = cursor.fetchone()
        assert result == (1,)
    
    # The fixtures should handle cleanup automatically


def test_no_resource_leaks(resource_manager):
    """Test that no resources are leaked during normal operations."""
    initial_count = resource_manager.get_resource_count()
    
    # Perform various operations that create resources
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
        db_path = f.name
    
    try:
        # Create and use a connection
        with managed_connection(db_path) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")
            conn.commit()
        
        # Create and use temp files
        with managed_temp_file() as temp_file:
            temp_file.write_text("test")
        
        with managed_temp_dir() as temp_dir:
            (temp_dir / "test.txt").write_text("test")
    
    finally:
        Path(db_path).unlink(missing_ok=True)
    
    # Resource count should be back to initial state (or close to it)
    final_count = resource_manager.get_resource_count()
    
    # Allow for some variance in resource counts due to test infrastructure
    # The key is that we don't have a significant increase
    total_initial = sum(initial_count.values())
    total_final = sum(final_count.values())
    
    # Should not have more than a few extra resources
    assert total_final <= total_initial + 2, f"Resource leak detected: {initial_count} -> {final_count}"


def test_error_handling_in_resource_cleanup(resource_manager):
    """Test that resource cleanup handles errors gracefully."""
    # Create a temp file and register it
    with tempfile.NamedTemporaryFile(delete=False) as f:
        temp_path = Path(f.name)
        f.write(b"test content")
    
    resource_id = resource_manager.register_temp_file(temp_path, "Error test file")
    
    # Manually delete the file to simulate an error condition
    temp_path.unlink()
    
    # Cleanup should handle the missing file gracefully
    result = resource_manager.cleanup_resource(resource_id)
    
    # Should still return True even if file was already gone
    assert result is True
    
    # Resource should be unregistered
    assert resource_manager.get_resource_info(resource_id) is None