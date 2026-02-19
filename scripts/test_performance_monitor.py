#!/usr/bin/env python3
"""
Test Performance Monitor for PySpark Tools

This script monitors test execution performance and provides insights for optimization.
"""

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


@dataclass
class TestPerformanceMetric:
    """Performance metric for a test."""

    test_name: str
    duration: float
    timestamp: float
    status: str
    parallel: bool = False
    cached: bool = False


class TestPerformanceMonitor:
    """Monitors and analyzes test performance over time."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = (
            db_path
            or Path.home() / ".cache" / "pyspark_tools_test" / "performance.sqlite"
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the performance tracking database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS test_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_name TEXT NOT NULL,
                    duration REAL NOT NULL,
                    timestamp REAL NOT NULL,
                    status TEXT NOT NULL,
                    parallel BOOLEAN DEFAULT FALSE,
                    cached BOOLEAN DEFAULT FALSE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_test_performance_name 
                ON test_performance(test_name)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_test_performance_timestamp 
                ON test_performance(timestamp)
            """
            )

    def record_test_performance(self, metric: TestPerformanceMetric) -> None:
        """Record a test performance metric."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO test_performance 
                (test_name, duration, timestamp, status, parallel, cached)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    metric.test_name,
                    metric.duration,
                    metric.timestamp,
                    metric.status,
                    metric.parallel,
                    metric.cached,
                ),
            )

    def get_test_performance_history(
        self, test_name: str, days: int = 30
    ) -> List[TestPerformanceMetric]:
        """Get performance history for a specific test."""
        cutoff_time = time.time() - (days * 24 * 3600)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT test_name, duration, timestamp, status, parallel, cached
                FROM test_performance
                WHERE test_name = ? AND timestamp > ?
                ORDER BY timestamp DESC
            """,
                (test_name, cutoff_time),
            )

            return [
                TestPerformanceMetric(
                    test_name=row[0],
                    duration=row[1],
                    timestamp=row[2],
                    status=row[3],
                    parallel=bool(row[4]),
                    cached=bool(row[5]),
                )
                for row in cursor.fetchall()
            ]

    def get_slowest_tests(self, limit: int = 10) -> List[Dict]:
        """Get the slowest tests based on recent performance."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 
                    test_name,
                    AVG(duration) as avg_duration,
                    MAX(duration) as max_duration,
                    MIN(duration) as min_duration,
                    COUNT(*) as run_count
                FROM test_performance
                WHERE timestamp > ? AND status = 'passed'
                GROUP BY test_name
                ORDER BY avg_duration DESC
                LIMIT ?
            """,
                (time.time() - (7 * 24 * 3600), limit),
            )

            return [
                {
                    "test_name": row[0],
                    "avg_duration": row[1],
                    "max_duration": row[2],
                    "min_duration": row[3],
                    "run_count": row[4],
                }
                for row in cursor.fetchall()
            ]

    def get_performance_trends(self) -> Dict:
        """Get performance trends over time."""
        with sqlite3.connect(self.db_path) as conn:
            # Overall trends
            cursor = conn.execute(
                """
                SELECT 
                    DATE(datetime(timestamp, 'unixepoch')) as date,
                    AVG(duration) as avg_duration,
                    COUNT(*) as test_count,
                    SUM(CASE WHEN parallel THEN 1 ELSE 0 END) as parallel_count,
                    SUM(CASE WHEN cached THEN 1 ELSE 0 END) as cached_count
                FROM test_performance
                WHERE timestamp > ?
                GROUP BY DATE(datetime(timestamp, 'unixepoch'))
                ORDER BY date DESC
                LIMIT 30
            """,
                (time.time() - (30 * 24 * 3600),),
            )

            daily_trends = [
                {
                    "date": row[0],
                    "avg_duration": row[1],
                    "test_count": row[2],
                    "parallel_count": row[3],
                    "cached_count": row[4],
                    "parallel_rate": row[3] / row[2] if row[2] > 0 else 0,
                    "cache_rate": row[4] / row[2] if row[2] > 0 else 0,
                }
                for row in cursor.fetchall()
            ]

            return {"daily_trends": daily_trends}

    def detect_performance_regressions(self, threshold: float = 1.5) -> List[Dict]:
        """Detect tests with performance regressions."""
        regressions = []

        with sqlite3.connect(self.db_path) as conn:
            # Get tests with significant duration increases
            cursor = conn.execute(
                """
                WITH recent_performance AS (
                    SELECT 
                        test_name,
                        AVG(duration) as recent_avg
                    FROM test_performance
                    WHERE timestamp > ? AND status = 'passed'
                    GROUP BY test_name
                ),
                historical_performance AS (
                    SELECT 
                        test_name,
                        AVG(duration) as historical_avg
                    FROM test_performance
                    WHERE timestamp BETWEEN ? AND ? AND status = 'passed'
                    GROUP BY test_name
                )
                SELECT 
                    r.test_name,
                    r.recent_avg,
                    h.historical_avg,
                    (r.recent_avg / h.historical_avg) as ratio
                FROM recent_performance r
                JOIN historical_performance h ON r.test_name = h.test_name
                WHERE r.recent_avg / h.historical_avg > ?
                ORDER BY ratio DESC
            """,
                (
                    time.time() - (7 * 24 * 3600),  # Recent: last 7 days
                    time.time() - (30 * 24 * 3600),  # Historical: 30-7 days ago
                    time.time() - (7 * 24 * 3600),
                    threshold,
                ),
            )

            for row in cursor.fetchall():
                regressions.append(
                    {
                        "test_name": row[0],
                        "recent_avg": row[1],
                        "historical_avg": row[2],
                        "slowdown_ratio": row[3],
                        "slowdown_percent": (row[3] - 1) * 100,
                    }
                )

        return regressions

    def generate_performance_report(self) -> Dict:
        """Generate a comprehensive performance report."""
        report = {
            "timestamp": time.time(),
            "slowest_tests": self.get_slowest_tests(10),
            "performance_trends": self.get_performance_trends(),
            "regressions": self.detect_performance_regressions(),
            "summary": {},
        }

        # Calculate summary statistics
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 
                    COUNT(*) as total_tests,
                    AVG(duration) as avg_duration,
                    MAX(duration) as max_duration,
                    SUM(CASE WHEN parallel THEN 1 ELSE 0 END) as parallel_tests,
                    SUM(CASE WHEN cached THEN 1 ELSE 0 END) as cached_tests
                FROM test_performance
                WHERE timestamp > ?
            """,
                (time.time() - (7 * 24 * 3600),),
            )

            row = cursor.fetchone()
            if row:
                report["summary"] = {
                    "total_tests": row[0],
                    "avg_duration": row[1],
                    "max_duration": row[2],
                    "parallel_rate": row[3] / row[0] if row[0] > 0 else 0,
                    "cache_rate": row[4] / row[0] if row[0] > 0 else 0,
                }

        return report

    def plot_performance_trends(self, output_path: Optional[Path] = None) -> None:
        """Plot performance trends over time."""
        trends = self.get_performance_trends()
        daily_data = trends["daily_trends"]

        if not daily_data:
            print("No performance data available for plotting")
            return

        df = pd.DataFrame(daily_data)
        df["date"] = pd.to_datetime(df["date"])

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

        # Average duration trend
        ax1.plot(df["date"], df["avg_duration"], marker="o")
        ax1.set_title("Average Test Duration Over Time")
        ax1.set_ylabel("Duration (seconds)")
        ax1.tick_params(axis="x", rotation=45)

        # Test count trend
        ax2.plot(df["date"], df["test_count"], marker="s", color="green")
        ax2.set_title("Number of Tests Run Per Day")
        ax2.set_ylabel("Test Count")
        ax2.tick_params(axis="x", rotation=45)

        # Parallel execution rate
        ax3.plot(df["date"], df["parallel_rate"] * 100, marker="^", color="orange")
        ax3.set_title("Parallel Execution Rate")
        ax3.set_ylabel("Parallel Rate (%)")
        ax3.tick_params(axis="x", rotation=45)

        # Cache hit rate
        ax4.plot(df["date"], df["cache_rate"] * 100, marker="d", color="purple")
        ax4.set_title("Cache Hit Rate")
        ax4.set_ylabel("Cache Rate (%)")
        ax4.tick_params(axis="x", rotation=45)

        plt.tight_layout()

        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            print(f"Performance trends plot saved to {output_path}")
        else:
            plt.show()


def main():
    """Main entry point for performance monitoring."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Performance Monitor")
    parser.add_argument(
        "--report", action="store_true", help="Generate performance report"
    )
    parser.add_argument(
        "--plot", action="store_true", help="Generate performance plots"
    )
    parser.add_argument("--slowest", type=int, default=10, help="Show N slowest tests")
    parser.add_argument(
        "--regressions", action="store_true", help="Show performance regressions"
    )
    parser.add_argument("--output", type=Path, help="Output file for reports/plots")

    args = parser.parse_args()

    monitor = TestPerformanceMonitor()

    if args.report:
        report = monitor.generate_performance_report()

        if args.output:
            with open(args.output, "w") as f:
                json.dump(report, f, indent=2)
            print(f"Performance report saved to {args.output}")
        else:
            print(json.dumps(report, indent=2))

    if args.plot:
        output_path = args.output if args.output else Path("performance_trends.png")
        monitor.plot_performance_trends(output_path)

    if args.slowest:
        slowest = monitor.get_slowest_tests(args.slowest)
        print(f"\nTop {args.slowest} Slowest Tests:")
        print("-" * 60)
        for test in slowest:
            print(f"{test['test_name']:<40} {test['avg_duration']:>8.2f}s (avg)")

    if args.regressions:
        regressions = monitor.detect_performance_regressions()
        if regressions:
            print("\nPerformance Regressions Detected:")
            print("-" * 60)
            for reg in regressions:
                print(f"{reg['test_name']:<40} {reg['slowdown_percent']:>6.1f}% slower")
        else:
            print("No performance regressions detected")


if __name__ == "__main__":
    main()
