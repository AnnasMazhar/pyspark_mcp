"""
Test Execution Optimizer for PySpark Tools

This module provides optimizations for test execution including:
- Parallel test execution for independent tests
- Smart caching for test data and fixtures
- Database operation optimization
- Test flakiness detection and reporting
"""

import json
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import Mock

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Represents the result of a test execution."""
    
    test_name: str
    status: str  # 'passed', 'failed', 'skipped', 'error'
    duration: float
    error_message: Optional[str] = None
    retry_count: int = 0
    flaky: bool = False


@dataclass
class TestExecutionStats:
    """Statistics for test execution optimization."""
    
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    skipped_tests: int = 0
    total_duration: float = 0.0
    parallel_speedup: float = 0.0
    cache_hit_rate: float = 0.0
    flaky_tests: List[str] = field(default_factory=list)


class TestDataCache:
    """Smart caching system for test data and fixtures."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or Path.home() / ".cache" / "pyspark_tools_test"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache: Dict[str, Any] = {}
        self._cache_stats = {"hits": 0, "misses": 0}
        self._lock = threading.Lock()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get cached data by key."""
        with self._lock:
            # Check memory cache first
            if key in self._memory_cache:
                self._cache_stats["hits"] += 1
                return self._memory_cache[key]
            
            # Check disk cache
            cache_file = self.cache_dir / f"{key}.json"
            if cache_file.exists():
                try:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                    self._memory_cache[key] = data
                    self._cache_stats["hits"] += 1
                    return data
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to load cache file {cache_file}: {e}")
            
            self._cache_stats["misses"] += 1
            return default
    
    def set(self, key: str, value: Any) -> None:
        """Set cached data by key."""
        with self._lock:
            self._memory_cache[key] = value
            
            # Also save to disk for persistence
            cache_file = self.cache_dir / f"{key}.json"
            try:
                with open(cache_file, 'w') as f:
                    json.dump(value, f, indent=2, default=str)
            except (TypeError, IOError) as e:
                logger.warning(f"Failed to save cache file {cache_file}: {e}")
    
    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            self._memory_cache.clear()
            for cache_file in self.cache_dir.glob("*.json"):
                try:
                    cache_file.unlink()
                except OSError:
                    pass
    
    def get_hit_rate(self) -> float:
        """Get cache hit rate as percentage."""
        total = self._cache_stats["hits"] + self._cache_stats["misses"]
        if total == 0:
            return 0.0
        return (self._cache_stats["hits"] / total) * 100.0


class DatabaseOptimizer:
    """Optimizes database operations for faster test execution."""
    
    def __init__(self):
        self._connection_pool: Dict[str, sqlite3.Connection] = {}
        self._pool_lock = threading.Lock()
    
    def get_optimized_connection(self, db_path: str) -> sqlite3.Connection:
        """Get an optimized database connection with performance settings."""
        with self._pool_lock:
            if db_path not in self._connection_pool:
                conn = sqlite3.connect(db_path, check_same_thread=False)
                
                # Apply performance optimizations
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.execute("PRAGMA cache_size = 10000")
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA mmap_size = 268435456")  # 256MB
                
                self._connection_pool[db_path] = conn
            
            return self._connection_pool[db_path]
    
    def create_test_database(self, db_path: str) -> sqlite3.Connection:
        """Create an optimized test database with indexes."""
        conn = self.get_optimized_connection(db_path)
        
        # Create basic tables if they don't exist (for testing)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversions (
                    id INTEGER PRIMARY KEY,
                    timestamp REAL,
                    sql_query TEXT,
                    pyspark_code TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS duplicate_patterns (
                    id INTEGER PRIMARY KEY,
                    usage_count INTEGER,
                    pattern_hash TEXT,
                    description TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    created_at DATETIME,
                    completed_at DATETIME
                )
            """)
            
            # Create indexes for common queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_conversions_timestamp 
                ON conversions(timestamp)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_patterns_usage_count 
                ON duplicate_patterns(usage_count)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_batch_jobs_status 
                ON batch_jobs(status)
            """)
            conn.commit()
        except sqlite3.Error as e:
            logger.warning(f"Failed to create test database schema: {e}")
        
        return conn
    
    def cleanup_connections(self) -> None:
        """Clean up all database connections."""
        with self._pool_lock:
            for conn in self._connection_pool.values():
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self._connection_pool.clear()


class FlakinessDetector:
    """Detects and reports flaky tests."""
    
    def __init__(self, history_file: Optional[Path] = None):
        self.history_file = history_file or Path.home() / ".cache" / "pyspark_tools_test" / "flaky_history.json"
        self.history_file.parent.mkdir(parents=True, exist_ok=True)
        self._test_history: Dict[str, List[str]] = self._load_history()
        self._lock = threading.Lock()
    
    def _load_history(self) -> Dict[str, List[str]]:
        """Load test execution history."""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}
    
    def _save_history(self) -> None:
        """Save test execution history."""
        try:
            with open(self.history_file, 'w') as f:
                json.dump(self._test_history, f, indent=2)
        except IOError as e:
            logger.warning(f"Failed to save flakiness history: {e}")
    
    def record_test_result(self, test_name: str, status: str) -> None:
        """Record a test result for flakiness analysis."""
        with self._lock:
            if test_name not in self._test_history:
                self._test_history[test_name] = []
            
            # Keep only last 10 results
            self._test_history[test_name].append(status)
            if len(self._test_history[test_name]) > 10:
                self._test_history[test_name] = self._test_history[test_name][-10:]
            
            self._save_history()
    
    def is_flaky(self, test_name: str, threshold: float = 0.3) -> bool:
        """Check if a test is considered flaky based on history."""
        if test_name not in self._test_history:
            return False
        
        history = self._test_history[test_name]
        if len(history) < 3:  # Need at least 3 runs to determine flakiness
            return False
        
        failure_rate = history.count('failed') / len(history)
        return 0 < failure_rate < (1 - threshold)  # Flaky if sometimes fails, sometimes passes
    
    def get_flaky_tests(self) -> List[str]:
        """Get list of all flaky tests."""
        return [test for test in self._test_history.keys() if self.is_flaky(test)]
    
    def get_flakiness_report(self) -> Dict[str, Any]:
        """Generate a comprehensive flakiness report."""
        flaky_tests = self.get_flaky_tests()
        report = {
            "total_tests_tracked": len(self._test_history),
            "flaky_tests_count": len(flaky_tests),
            "flaky_tests": []
        }
        
        for test in flaky_tests:
            history = self._test_history[test]
            failure_rate = history.count('failed') / len(history)
            report["flaky_tests"].append({
                "name": test,
                "failure_rate": failure_rate,
                "recent_results": history[-5:],  # Last 5 results
                "total_runs": len(history)
            })
        
        return report


class ParallelTestExecutor:
    """Executes tests in parallel where safe."""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or min(4, (threading.active_count() + 4))
        self.cache = TestDataCache()
        self.db_optimizer = DatabaseOptimizer()
        self.flakiness_detector = FlakinessDetector()
        
        # Tests that cannot be run in parallel (use shared resources)
        self.sequential_tests = {
            "test_server.py",  # Uses server ports
            "test_integration_mcp.py",  # Uses MCP server
            "test_resource_manager.py",  # Tests resource management itself
        }
    
    def _is_safe_for_parallel(self, test_file: str) -> bool:
        """Check if a test file is safe to run in parallel."""
        test_name = Path(test_file).name
        return test_name not in self.sequential_tests
    
    def _run_single_test(self, test_file: str, pytest_args: List[str]) -> TestResult:
        """Run a single test file and return results."""
        import subprocess
        
        start_time = time.time()
        test_name = Path(test_file).name
        
        try:
            # Build pytest command
            cmd = ["python", "-m", "pytest", test_file] + pytest_args
            
            # Run test
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per test file
            )
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                status = "passed"
                error_message = None
            else:
                status = "failed"
                error_message = result.stderr or result.stdout
            
            # Record result for flakiness detection
            self.flakiness_detector.record_test_result(test_name, status)
            
            test_result = TestResult(
                test_name=test_name,
                status=status,
                duration=duration,
                error_message=error_message,
                flaky=self.flakiness_detector.is_flaky(test_name)
            )
            
            # Log error details for debugging
            if status == "failed" and error_message:
                logger.error(f"Test {test_name} failed with error: {error_message}")
            
            return test_result
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            self.flakiness_detector.record_test_result(test_name, "timeout")
            return TestResult(
                test_name=test_name,
                status="timeout",
                duration=duration,
                error_message="Test timed out after 5 minutes"
            )
        except Exception as e:
            duration = time.time() - start_time
            self.flakiness_detector.record_test_result(test_name, "error")
            return TestResult(
                test_name=test_name,
                status="error",
                duration=duration,
                error_message=str(e)
            )
    
    def execute_tests(self, test_files: List[str], pytest_args: Optional[List[str]] = None) -> TestExecutionStats:
        """Execute tests with optimal parallelization."""
        if pytest_args is None:
            pytest_args = ["-v", "--tb=short"]
        
        start_time = time.time()
        
        # Separate parallel and sequential tests
        parallel_tests = [f for f in test_files if self._is_safe_for_parallel(f)]
        sequential_tests = [f for f in test_files if not self._is_safe_for_parallel(f)]
        
        results: List[TestResult] = []
        
        # Run parallel tests
        if parallel_tests:
            logger.info(f"Running {len(parallel_tests)} tests in parallel with {self.max_workers} workers")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_test = {
                    executor.submit(self._run_single_test, test_file, pytest_args): test_file
                    for test_file in parallel_tests
                }
                
                for future in as_completed(future_to_test):
                    result = future.result()
                    results.append(result)
                    logger.info(f"Completed {result.test_name}: {result.status} ({result.duration:.2f}s)")
        
        # Run sequential tests
        if sequential_tests:
            logger.info(f"Running {len(sequential_tests)} tests sequentially")
            for test_file in sequential_tests:
                result = self._run_single_test(test_file, pytest_args)
                results.append(result)
                logger.info(f"Completed {result.test_name}: {result.status} ({result.duration:.2f}s)")
        
        total_duration = time.time() - start_time
        
        # Calculate statistics
        stats = TestExecutionStats(
            total_tests=len(results),
            passed_tests=len([r for r in results if r.status == "passed"]),
            failed_tests=len([r for r in results if r.status == "failed"]),
            skipped_tests=len([r for r in results if r.status == "skipped"]),
            total_duration=total_duration,
            cache_hit_rate=self.cache.get_hit_rate(),
            flaky_tests=[r.test_name for r in results if r.flaky]
        )
        
        # Calculate parallel speedup estimate
        sequential_duration = sum(r.duration for r in results)
        if sequential_duration > 0:
            stats.parallel_speedup = sequential_duration / total_duration
        
        return stats
    
    def cleanup(self) -> None:
        """Clean up resources."""
        self.db_optimizer.cleanup_connections()


class TestOptimizer:
    """Main test optimization coordinator."""
    
    def __init__(self):
        self.executor = ParallelTestExecutor()
        self.cache = TestDataCache()
        self.db_optimizer = DatabaseOptimizer()
        self.flakiness_detector = FlakinessDetector()
    
    def optimize_test_execution(self, test_files: List[str], **kwargs) -> TestExecutionStats:
        """Execute tests with all optimizations enabled."""
        logger.info("Starting optimized test execution")
        
        # Pre-warm cache with common test data
        self._prewarm_cache()
        
        # Execute tests
        stats = self.executor.execute_tests(test_files, **kwargs)
        
        # Generate reports
        self._generate_optimization_report(stats)
        
        return stats
    
    def _prewarm_cache(self) -> None:
        """Pre-warm cache with commonly used test data."""
        # Cache common SQL queries
        common_queries = {
            "simple_select": "SELECT * FROM users WHERE id = 1",
            "join_query": "SELECT u.*, p.title FROM users u JOIN profiles p ON u.id = p.user_id",
            "aggregate_query": "SELECT COUNT(*), AVG(age) FROM users GROUP BY department"
        }
        
        for key, query in common_queries.items():
            if self.cache.get(f"sql_query_{key}") is None:
                self.cache.set(f"sql_query_{key}", query)
        
        # Cache mock objects
        mock_spark_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
        self.cache.set("mock_spark_config", mock_spark_config)
    
    def _generate_optimization_report(self, stats: TestExecutionStats) -> None:
        """Generate and save optimization report."""
        report = {
            "execution_stats": {
                "total_tests": stats.total_tests,
                "passed_tests": stats.passed_tests,
                "failed_tests": stats.failed_tests,
                "total_duration": stats.total_duration,
                "parallel_speedup": stats.parallel_speedup,
                "cache_hit_rate": stats.cache_hit_rate
            },
            "flakiness_report": self.flakiness_detector.get_flakiness_report(),
            "optimization_recommendations": self._get_optimization_recommendations(stats)
        }
        
        report_file = Path("test_optimization_report.json")
        try:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Optimization report saved to {report_file}")
        except IOError as e:
            logger.warning(f"Failed to save optimization report: {e}")
    
    def _get_optimization_recommendations(self, stats: TestExecutionStats) -> List[str]:
        """Generate optimization recommendations based on stats."""
        recommendations = []
        
        if stats.cache_hit_rate < 50:
            recommendations.append("Consider adding more test data to cache for better performance")
        
        if stats.parallel_speedup < 2.0:
            recommendations.append("Some tests may benefit from better parallelization")
        
        if stats.flaky_tests:
            recommendations.append(f"Address {len(stats.flaky_tests)} flaky tests for better reliability")
        
        if stats.total_duration > 300:  # 5 minutes
            recommendations.append("Consider splitting long-running tests into smaller units")
        
        return recommendations
    
    def cleanup(self) -> None:
        """Clean up all optimization resources."""
        self.executor.cleanup()
        self.db_optimizer.cleanup_connections()