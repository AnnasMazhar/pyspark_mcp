#!/usr/bin/env python3
"""
Performance Benchmarking Script for PySpark Tools MCP Server

This script validates that the system meets all performance targets defined
in the v1.0 release requirements.

Performance Targets:
- Conversion Speed: <2 seconds for typical SQL queries
- Startup Time: <5 seconds for server initialization
- Batch Processing: 100+ files processed concurrently
- Memory Efficiency: <512MB for typical workloads
- Accuracy: >95% successful conversion for common SQL patterns
- Reliability: <1% error rate in production workloads
"""

import asyncio
import json
import os
import psutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark_tools.sql_converter import SQLConverter
from pyspark_tools.batch_processor import BatchProcessor
from pyspark_tools.memory_manager import MemoryManager
from pyspark_tools.server import create_server


class PerformanceBenchmark:
    """Performance benchmarking suite for PySpark Tools."""
    
    def __init__(self):
        self.results = {}
        self.memory_manager = MemoryManager()
        self.sql_converter = SQLConverter(self.memory_manager)
        self.batch_processor = BatchProcessor(self.memory_manager)
        
    def measure_memory_usage(self, func, *args, **kwargs):
        """Measure memory usage during function execution."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        peak_memory = process.memory_info().peak_wss / 1024 / 1024 if hasattr(process.memory_info(), 'peak_wss') else final_memory
        
        return {
            'result': result,
            'execution_time': end_time - start_time,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'peak_memory_mb': peak_memory,
            'memory_delta_mb': final_memory - initial_memory
        }
    
    def benchmark_server_startup(self) -> Dict:
        """Benchmark server startup time."""
        print("🚀 Benchmarking server startup time...")
        
        def start_server():
            server = create_server()
            return server
        
        metrics = self.measure_memory_usage(start_server)
        
        result = {
            'startup_time_seconds': metrics['execution_time'],
            'startup_memory_mb': metrics['memory_delta_mb'],
            'target_seconds': 5.0,
            'passed': metrics['execution_time'] < 5.0
        }
        
        print(f"   Startup time: {result['startup_time_seconds']:.2f}s (target: <5s)")
        print(f"   Memory usage: {result['startup_memory_mb']:.2f}MB")
        print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
        
        return result
    
    def benchmark_sql_conversion_speed(self) -> Dict:
        """Benchmark SQL to PySpark conversion speed."""
        print("⚡ Benchmarking SQL conversion speed...")
        
        # Test queries of varying complexity
        test_queries = [
            # Simple query
            "SELECT id, name FROM users WHERE age > 25",
            
            # Medium complexity with joins
            """
            SELECT u.name, p.title, COUNT(*) as post_count
            FROM users u
            JOIN posts p ON u.id = p.user_id
            WHERE u.created_at > '2023-01-01'
            GROUP BY u.name, p.title
            ORDER BY post_count DESC
            """,
            
            # Complex query with CTEs and window functions
            """
            WITH user_stats AS (
                SELECT 
                    user_id,
                    COUNT(*) as total_posts,
                    AVG(likes) as avg_likes,
                    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
                FROM posts
                WHERE created_at >= '2023-01-01'
                GROUP BY user_id
            ),
            top_users AS (
                SELECT user_id, total_posts, avg_likes
                FROM user_stats
                WHERE rank <= 10
            )
            SELECT 
                u.name,
                tu.total_posts,
                tu.avg_likes,
                CASE 
                    WHEN tu.avg_likes > 100 THEN 'High Engagement'
                    WHEN tu.avg_likes > 50 THEN 'Medium Engagement'
                    ELSE 'Low Engagement'
                END as engagement_level
            FROM top_users tu
            JOIN users u ON tu.user_id = u.id
            ORDER BY tu.total_posts DESC
            """
        ]
        
        conversion_times = []
        success_count = 0
        
        for i, query in enumerate(test_queries):
            print(f"   Testing query {i+1}/{len(test_queries)}...")
            
            def convert_query():
                return self.sql_converter.convert_sql_to_pyspark(query)
            
            metrics = self.measure_memory_usage(convert_query)
            conversion_times.append(metrics['execution_time'])
            
            if metrics['result']['success']:
                success_count += 1
            
            print(f"     Time: {metrics['execution_time']:.3f}s")
        
        avg_time = sum(conversion_times) / len(conversion_times)
        max_time = max(conversion_times)
        success_rate = (success_count / len(test_queries)) * 100
        
        result = {
            'average_conversion_time_seconds': avg_time,
            'max_conversion_time_seconds': max_time,
            'success_rate_percent': success_rate,
            'target_seconds': 2.0,
            'target_success_rate': 95.0,
            'time_passed': max_time < 2.0,
            'accuracy_passed': success_rate >= 95.0,
            'passed': max_time < 2.0 and success_rate >= 95.0
        }
        
        print(f"   Average time: {avg_time:.3f}s")
        print(f"   Max time: {max_time:.3f}s (target: <2s)")
        print(f"   Success rate: {success_rate:.1f}% (target: >95%)")
        print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
        
        return result
    
    def benchmark_batch_processing(self) -> Dict:
        """Benchmark batch processing capabilities."""
        print("📦 Benchmarking batch processing...")
        
        # Create temporary SQL files for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Generate test SQL files
            test_files = []
            for i in range(150):  # Test with 150 files (target: 100+)
                file_path = temp_path / f"query_{i:03d}.sql"
                with open(file_path, 'w') as f:
                    f.write(f"""
                    SELECT 
                        id,
                        name,
                        email,
                        created_at,
                        COUNT(*) OVER (PARTITION BY department) as dept_count
                    FROM employees_{i % 10}
                    WHERE salary > {20000 + (i * 100)}
                    ORDER BY created_at DESC
                    LIMIT {10 + (i % 20)}
                    """)
                test_files.append(str(file_path))
            
            # Benchmark batch processing
            def process_batch():
                return self.batch_processor.process_files(
                    test_files[:100],  # Process 100 files
                    str(temp_path / "output")
                )
            
            metrics = self.measure_memory_usage(process_batch)
            batch_result = metrics['result']
            
            # Test concurrent processing with more files
            def process_concurrent():
                return self.batch_processor.process_files(
                    test_files,  # Process all 150 files
                    str(temp_path / "output_concurrent")
                )
            
            concurrent_metrics = self.measure_memory_usage(process_concurrent)
            concurrent_result = concurrent_metrics['result']
            
            result = {
                'batch_100_files_seconds': metrics['execution_time'],
                'batch_100_success_rate': (batch_result['successful_conversions'] / 100) * 100,
                'concurrent_150_files_seconds': concurrent_metrics['execution_time'],
                'concurrent_150_success_rate': (concurrent_result['successful_conversions'] / 150) * 100,
                'memory_usage_mb': max(metrics['peak_memory_mb'], concurrent_metrics['peak_memory_mb']),
                'target_files': 100,
                'target_memory_mb': 512,
                'files_passed': concurrent_result['successful_conversions'] >= 100,
                'memory_passed': max(metrics['peak_memory_mb'], concurrent_metrics['peak_memory_mb']) < 512,
                'passed': (concurrent_result['successful_conversions'] >= 100 and 
                          max(metrics['peak_memory_mb'], concurrent_metrics['peak_memory_mb']) < 512)
            }
            
            print(f"   100 files processed in: {result['batch_100_files_seconds']:.2f}s")
            print(f"   100 files success rate: {result['batch_100_success_rate']:.1f}%")
            print(f"   150 files processed in: {result['concurrent_150_files_seconds']:.2f}s")
            print(f"   150 files success rate: {result['concurrent_150_success_rate']:.1f}%")
            print(f"   Peak memory usage: {result['memory_usage_mb']:.1f}MB (target: <512MB)")
            print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
            
            return result
    
    def benchmark_memory_efficiency(self) -> Dict:
        """Benchmark memory efficiency under typical workloads."""
        print("💾 Benchmarking memory efficiency...")
        
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Simulate typical workload
        queries = [
            "SELECT * FROM table1 WHERE id > 100",
            "SELECT a.*, b.name FROM table1 a JOIN table2 b ON a.id = b.id",
            "SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
            "WITH cte AS (SELECT * FROM sales WHERE date > '2023-01-01') SELECT * FROM cte",
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees"
        ]
        
        # Process multiple batches to simulate sustained usage
        for batch in range(10):
            for query in queries:
                self.sql_converter.convert_sql_to_pyspark(query)
        
        # Force garbage collection
        import gc
        gc.collect()
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_usage = final_memory - initial_memory
        
        result = {
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'memory_usage_mb': memory_usage,
            'target_memory_mb': 512,
            'passed': memory_usage < 512
        }
        
        print(f"   Initial memory: {initial_memory:.1f}MB")
        print(f"   Final memory: {final_memory:.1f}MB")
        print(f"   Memory usage: {memory_usage:.1f}MB (target: <512MB)")
        print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
        
        return result
    
    def benchmark_error_rate(self) -> Dict:
        """Benchmark error rate and reliability."""
        print("🔍 Benchmarking error rate and reliability...")
        
        # Test with various SQL patterns including edge cases
        test_cases = [
            # Valid queries that should succeed
            "SELECT id, name FROM users",
            "SELECT COUNT(*) FROM orders WHERE date > '2023-01-01'",
            "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            
            # Complex but valid queries
            "WITH sales_data AS (SELECT * FROM sales) SELECT * FROM sales_data",
            "SELECT *, ROW_NUMBER() OVER (ORDER BY created_at) FROM posts",
            
            # Edge cases that might cause issues
            "SELECT * FROM table_with_very_long_name_that_might_cause_issues",
            "SELECT 'string with spaces' as col1, 123 as col2",
            "SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END FROM numbers",
            
            # Potentially problematic queries (should handle gracefully)
            "SELECT * FROM",  # Incomplete query
            "INVALID SQL QUERY",  # Invalid syntax
            "",  # Empty query
            "SELECT * FROM non_existent_table WHERE complex_condition = 'test'",
        ]
        
        total_queries = len(test_cases)
        successful_conversions = 0
        graceful_failures = 0
        
        for i, query in enumerate(test_cases):
            try:
                result = self.sql_converter.convert_sql_to_pyspark(query)
                if result['success']:
                    successful_conversions += 1
                else:
                    # Check if failure was handled gracefully
                    if 'error_message' in result and result['error_message']:
                        graceful_failures += 1
            except Exception as e:
                # Unexpected exceptions are not graceful failures
                print(f"     Unexpected error on query {i+1}: {e}")
        
        # Calculate reliability metrics
        total_handled = successful_conversions + graceful_failures
        reliability_rate = (total_handled / total_queries) * 100
        error_rate = ((total_queries - total_handled) / total_queries) * 100
        
        result = {
            'total_queries': total_queries,
            'successful_conversions': successful_conversions,
            'graceful_failures': graceful_failures,
            'unexpected_errors': total_queries - total_handled,
            'reliability_rate_percent': reliability_rate,
            'error_rate_percent': error_rate,
            'target_error_rate': 1.0,
            'passed': error_rate < 1.0
        }
        
        print(f"   Total queries tested: {total_queries}")
        print(f"   Successful conversions: {successful_conversions}")
        print(f"   Graceful failures: {graceful_failures}")
        print(f"   Unexpected errors: {result['unexpected_errors']}")
        print(f"   Reliability rate: {reliability_rate:.1f}%")
        print(f"   Error rate: {error_rate:.1f}% (target: <1%)")
        print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
        
        return result
    
    def run_all_benchmarks(self) -> Dict:
        """Run all performance benchmarks."""
        print("🏁 Starting Performance Benchmark Suite")
        print("=" * 50)
        
        benchmarks = {
            'server_startup': self.benchmark_server_startup,
            'sql_conversion_speed': self.benchmark_sql_conversion_speed,
            'batch_processing': self.benchmark_batch_processing,
            'memory_efficiency': self.benchmark_memory_efficiency,
            'error_rate': self.benchmark_error_rate
        }
        
        results = {}
        all_passed = True
        
        for name, benchmark_func in benchmarks.items():
            print(f"\n{name.replace('_', ' ').title()}:")
            try:
                result = benchmark_func()
                results[name] = result
                if not result['passed']:
                    all_passed = False
            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                results[name] = {'passed': False, 'error': str(e)}
                all_passed = False
        
        # Overall results
        print("\n" + "=" * 50)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 50)
        
        for name, result in results.items():
            status = "✅ PASSED" if result['passed'] else "❌ FAILED"
            print(f"{name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall Status: {'✅ ALL BENCHMARKS PASSED' if all_passed else '❌ SOME BENCHMARKS FAILED'}")
        
        # Save results to file
        results_file = Path(__file__).parent.parent / "benchmark_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': time.time(),
                'overall_passed': all_passed,
                'results': results
            }, f, indent=2)
        
        print(f"\n📄 Detailed results saved to: {results_file}")
        
        return {
            'overall_passed': all_passed,
            'results': results
        }


def main():
    """Main entry point for performance benchmarking."""
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    
    # Exit with appropriate code
    sys.exit(0 if results['overall_passed'] else 1)


if __name__ == "__main__":
    main()