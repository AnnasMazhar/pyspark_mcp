"""
Tests for test optimization features.

This module tests the test optimization infrastructure including:
- Test data caching
- Database optimization
- Performance monitoring
- Parallel execution safety
"""

import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from test_optimizer import (
    DatabaseOptimizer,
    FlakinessDetector,
    TestDataCache,
    TestExecutionStats,
    TestOptimizer,
    TestResult,
)


class TestTestDataCache:
    """Test the test data caching system."""
    
    def test_cache_basic_operations(self, tmp_path):
        """Test basic cache operations."""
        cache = TestDataCache(tmp_path / "test_cache")
        
        # Test set and get
        test_data = {"key": "value", "number": 42}
        cache.set("test_key", test_data)
        
        retrieved = cache.get("test_key")
        assert retrieved == test_data
        
        # Test default value
        assert cache.get("nonexistent", "default") == "default"
    
    def test_cache_persistence(self, tmp_path):
        """Test that cache persists across instances."""
        cache_dir = tmp_path / "persistent_cache"
        
        # First cache instance
        cache1 = TestDataCache(cache_dir)
        cache1.set("persistent_key", "persistent_value")
        
        # Second cache instance
        cache2 = TestDataCache(cache_dir)
        assert cache2.get("persistent_key") == "persistent_value"
    
    def test_cache_hit_rate(self, tmp_path):
        """Test cache hit rate calculation."""
        cache = TestDataCache(tmp_path / "hit_rate_cache")
        
        # Initial hit rate should be 0
        assert cache.get_hit_rate() == 0.0
        
        # Add some data
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        # Hit existing keys
        cache.get("key1")
        cache.get("key2")
        
        # Miss non-existent key
        cache.get("key3")
        
        # Hit rate should be 66.7% (2 hits, 1 miss)
        hit_rate = cache.get_hit_rate()
        assert 60 <= hit_rate <= 70  # Allow some tolerance
    
    def test_cache_thread_safety(self, tmp_path):
        """Test that cache is thread-safe."""
        cache = TestDataCache(tmp_path / "thread_safe_cache")
        results = []
        
        def worker(thread_id):
            for i in range(10):
                key = f"thread_{thread_id}_key_{i}"
                value = f"thread_{thread_id}_value_{i}"
                cache.set(key, value)
                retrieved = cache.get(key)
                results.append(retrieved == value)
        
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All operations should have succeeded
        assert all(results)


class TestDatabaseOptimizer:
    """Test the database optimization system."""
    
    def test_optimized_connection(self, tmp_path):
        """Test optimized database connection creation."""
        optimizer = DatabaseOptimizer()
        db_path = str(tmp_path / "test_optimized.db")
        
        conn = optimizer.get_optimized_connection(db_path)
        assert conn is not None
        
        # Test that we get the same connection for the same path
        conn2 = optimizer.get_optimized_connection(db_path)
        assert conn is conn2
        
        optimizer.cleanup_connections()
    
    def test_test_database_creation(self, tmp_path):
        """Test optimized test database creation with indexes."""
        optimizer = DatabaseOptimizer()
        db_path = str(tmp_path / "test_with_indexes.db")
        
        conn = optimizer.create_test_database(db_path)
        
        # Check that indexes were created (they should not raise errors)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = cursor.fetchall()
        
        # Should have at least some indexes
        assert len(indexes) > 0
        
        optimizer.cleanup_connections()


class TestFlakinessDetector:
    """Test the flakiness detection system."""
    
    def test_flakiness_detection(self, tmp_path):
        """Test flaky test detection."""
        detector = FlakinessDetector(tmp_path / "flaky_history.json")
        
        # Record consistent passing test
        for _ in range(5):
            detector.record_test_result("stable_test", "passed")
        
        # Record flaky test (sometimes passes, sometimes fails)
        results = ["passed", "failed", "passed", "failed", "passed"]
        for result in results:
            detector.record_test_result("flaky_test", result)
        
        # Record consistently failing test
        for _ in range(5):
            detector.record_test_result("broken_test", "failed")
        
        # Check flakiness detection
        assert not detector.is_flaky("stable_test")
        assert detector.is_flaky("flaky_test")
        assert not detector.is_flaky("broken_test")  # Consistently failing is not flaky
    
    def test_flakiness_report(self, tmp_path):
        """Test flakiness report generation."""
        detector = FlakinessDetector(tmp_path / "report_history.json")
        
        # Add some test data
        detector.record_test_result("test1", "passed")
        detector.record_test_result("test1", "failed")
        detector.record_test_result("test1", "passed")
        
        report = detector.get_flakiness_report()
        
        assert "total_tests_tracked" in report
        assert "flaky_tests_count" in report
        assert "flaky_tests" in report
        assert report["total_tests_tracked"] >= 1


class TestTestOptimizer:
    """Test the main test optimizer."""
    
    def test_optimizer_initialization(self):
        """Test that optimizer initializes correctly."""
        optimizer = TestOptimizer()
        
        assert optimizer.executor is not None
        assert optimizer.cache is not None
        assert optimizer.db_optimizer is not None
        assert optimizer.flakiness_detector is not None
        
        optimizer.cleanup()
    
    def test_optimization_recommendations(self):
        """Test optimization recommendation generation."""
        optimizer = TestOptimizer()
        
        # Create mock stats
        stats = TestExecutionStats(
            total_tests=10,
            passed_tests=8,
            failed_tests=2,
            total_duration=120.0,
            parallel_speedup=1.5,
            cache_hit_rate=30.0,
            flaky_tests=["flaky_test1"]
        )
        
        recommendations = optimizer._get_optimization_recommendations(stats)
        
        # Should have recommendations for low cache hit rate and flaky tests
        assert len(recommendations) > 0
        assert any("cache" in rec.lower() for rec in recommendations)
        assert any("flaky" in rec.lower() for rec in recommendations)
        
        optimizer.cleanup()


@pytest.mark.performance
class TestPerformanceMonitoring:
    """Test performance monitoring features."""
    
    def test_performance_fixture(self, monitor_test_performance):
        """Test that performance monitoring fixture works."""
        # This test should be monitored for performance
        time.sleep(0.1)  # Small delay to test monitoring
        assert True
    
    def test_cached_data_fixture(self, cached_mock_data):
        """Test that cached mock data fixture works."""
        assert "users" in cached_mock_data
        assert "profiles" in cached_mock_data
        assert "orders" in cached_mock_data
        
        # Data should be consistent across calls
        assert len(cached_mock_data["users"]) == 3
        assert len(cached_mock_data["profiles"]) == 2
        assert len(cached_mock_data["orders"]) == 3


@pytest.mark.parallel_safe
class TestParallelSafety:
    """Test parallel execution safety markers."""
    
    def test_parallel_safe_test(self):
        """This test is marked as parallel-safe."""
        # This test doesn't use shared resources
        result = 2 + 2
        assert result == 4
    
    def test_another_parallel_safe_test(self):
        """Another parallel-safe test."""
        # Independent computation
        data = [1, 2, 3, 4, 5]
        assert sum(data) == 15


@pytest.mark.sequential_only
class TestSequentialOnly:
    """Test sequential-only execution markers."""
    
    def test_sequential_only_test(self):
        """This test must run sequentially."""
        # This test might use shared resources like ports or files
        assert True


@pytest.mark.fast
class TestFastTests:
    """Fast tests that should complete quickly."""
    
    def test_fast_computation(self):
        """A fast test."""
        result = sum(range(100))
        assert result == 4950
    
    def test_another_fast_test(self):
        """Another fast test."""
        assert "hello".upper() == "HELLO"


@pytest.mark.slow
class TestSlowTests:
    """Slow tests for testing performance monitoring."""
    
    def test_slow_operation(self):
        """A deliberately slow test."""
        time.sleep(0.5)  # Simulate slow operation
        assert True


@pytest.mark.cached
class TestCachedTests:
    """Tests that use caching optimizations."""
    
    def test_with_cached_data(self, tmp_path):
        """Test that uses cached data."""
        # Create test cache instance
        from test_optimizer import TestDataCache
        test_cache = TestDataCache(tmp_path)
        test_cache.set("test_key", "test_value")
        assert test_cache.get("test_key") == "test_value"