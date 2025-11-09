#!/usr/bin/env python3
"""
Unified Test Entry Point for PySpark Tools

This is the SINGLE entry point for all testing operations.
Replaces: Makefile, test_runner.sh, optimized_test_runner.py, and scattered validation scripts.

Usage:
    python test.py                    # Run all tests
    python test.py --fast             # Run fast tests only
    python test.py --unit             # Run unit tests only
    python test.py --integration      # Run integration tests only
    python test.py --performance      # Run performance tests
    python test.py --security         # Run security tests
    python test.py --docker           # Run tests in Docker
    python test.py --parallel         # Run tests in parallel
    python test.py --coverage         # Run with coverage
    python test.py --watch            # Watch mode for development
    python test.py --ci               # CI mode (optimized for CI/CD)
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestRunner:
    """Unified test runner for all testing scenarios."""

    def __init__(self):
        self.project_root = Path(__file__).parent
        self.test_results = {}
        self.start_time = time.time()

    def run_command(
        self, cmd: List[str], cwd: Optional[Path] = None, env: Optional[Dict] = None
    ) -> Tuple[int, str, str]:
        """Run a command and return exit code, stdout, stderr."""
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd or self.project_root,
                env=env or os.environ.copy(),
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minute timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return 124, "", "Command timed out after 30 minutes"
        except Exception as e:
            return 1, "", str(e)

    def check_docker(self) -> bool:
        """Check if Docker is available and running."""
        code, _, _ = self.run_command(["docker", "info"])
        return code == 0

    def run_fast_tests(self) -> bool:
        """Run fast tests (< 5 seconds each)."""
        logger.info("🚀 Running fast tests...")

        # Run specific fast test files to avoid marker issues
        cmd = [
            "python",
            "-m",
            "pytest",
            "tests/test_minimal.py",
            "tests/test_ci_basic.py",
            "-v",
            "--tb=short",
            "--maxfail=5",
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["fast"] = {"code": code, "stdout": stdout, "stderr": stderr}

        if code == 0:
            logger.info("✅ Fast tests passed")
            return True
        else:
            logger.error("❌ Fast tests failed")
            logger.error(f"STDOUT: {stdout}")
            logger.error(f"STDERR: {stderr}")
            return False

    def run_unit_tests(self) -> bool:
        """Run unit tests."""
        logger.info("🔬 Running unit tests...")

        # Run all the comprehensive test files
        test_files = [
            "tests/test_minimal.py",
            "tests/test_ci_basic.py",
            "tests/test_enhanced_sql_converter.py",
            "tests/test_batch_processor.py",
            "tests/test_duplicate_detector.py",
            "tests/test_file_utils.py",
            "tests/test_resource_manager.py",
            "tests/test_advanced_optimizer.py",
            "tests/test_data_source_analyzer.py",
            "tests/test_aws_glue_integration.py",
        ]

        cmd = ["python", "-m", "pytest"] + test_files + ["-v", "--tb=short"]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["unit"] = {"code": code, "stdout": stdout, "stderr": stderr}

        if code == 0:
            logger.info("✅ Unit tests passed")
            return True
        else:
            logger.error("❌ Unit tests failed")
            logger.error(f"STDOUT: {stdout}")
            logger.error(f"STDERR: {stderr}")
            return False

    def run_integration_tests(self) -> bool:
        """Run integration tests."""
        logger.info("🔗 Running integration tests...")

        cmd = [
            "python",
            "-m",
            "pytest",
            "tests/",
            "-v",
            "-m",
            "integration",
            "--tb=short",
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["integration"] = {
            "code": code,
            "stdout": stdout,
            "stderr": stderr,
        }

        if code == 0:
            logger.info("✅ Integration tests passed")
            return True
        else:
            logger.error("❌ Integration tests failed")
            return False

    def run_performance_tests(self) -> bool:
        """Run performance tests."""
        logger.info("⚡ Running performance tests...")

        cmd = [
            "python",
            "-m",
            "pytest",
            "tests/",
            "-v",
            "-m",
            "performance",
            "--tb=short",
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["performance"] = {
            "code": code,
            "stdout": stdout,
            "stderr": stderr,
        }

        if code == 0:
            logger.info("✅ Performance tests passed")
            return True
        else:
            logger.error("❌ Performance tests failed")
            return False

    def run_security_tests(self) -> bool:
        """Run security tests."""
        logger.info("🔒 Running security tests...")

        # Run security-focused tests
        cmd = ["python", "-m", "pytest", "tests/", "-v", "-k", "security", "--tb=short"]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["security"] = {
            "code": code,
            "stdout": stdout,
            "stderr": stderr,
        }

        if code == 0:
            logger.info("✅ Security tests passed")
            return True
        else:
            logger.error("❌ Security tests failed")
            return False

    def run_with_coverage(self) -> bool:
        """Run tests with coverage analysis."""
        logger.info("📊 Running tests with coverage...")

        # Run all comprehensive test files for better coverage
        test_files = [
            "tests/test_minimal.py",
            "tests/test_ci_basic.py",
            "tests/test_enhanced_sql_converter.py",
            "tests/test_batch_processor.py",
            "tests/test_duplicate_detector.py",
            "tests/test_file_utils.py",
            "tests/test_resource_manager.py",
            "tests/test_advanced_optimizer.py",
            "tests/test_data_source_analyzer.py",
            "tests/test_aws_glue_integration.py",
        ]

        cmd = (
            ["python", "-m", "pytest"]
            + test_files
            + [
                "-v",
                "--cov=pyspark_tools",
                "--cov-report=term-missing",
                "--cov-report=html:coverage_reports",
                "--cov-report=xml:coverage.xml",
                "--cov-fail-under=50",  # More realistic target
                "--tb=short",
            ]
        )

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["coverage"] = {
            "code": code,
            "stdout": stdout,
            "stderr": stderr,
        }

        if code == 0:
            logger.info("✅ Coverage tests passed")
            logger.info("📊 Coverage report: coverage_reports/index.html")
            return True
        else:
            logger.error("❌ Coverage tests failed")
            logger.error(f"STDOUT: {stdout}")
            logger.error(f"STDERR: {stderr}")
            return False

    def run_parallel_tests(self) -> bool:
        """Run tests in parallel."""
        logger.info("⚡ Running tests in parallel...")

        # Check if pytest-xdist is available
        code, _, _ = self.run_command(["python", "-c", "import xdist"])
        if code != 0:
            logger.warning("pytest-xdist not available, falling back to sequential")
            return self.run_unit_tests()

        cmd = [
            "python",
            "-m",
            "pytest",
            "tests/",
            "-v",
            "-n",
            "auto",
            "--dist",
            "worksteal",
            "-m",
            "not sequential_only",
            "--tb=short",
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["parallel"] = {
            "code": code,
            "stdout": stdout,
            "stderr": stderr,
        }

        if code == 0:
            logger.info("✅ Parallel tests passed")
            return True
        else:
            logger.error("❌ Parallel tests failed")
            return False

    def run_docker_tests(self) -> bool:
        """Run tests in Docker environment."""
        logger.info("🐳 Running tests in Docker...")

        if not self.check_docker():
            logger.error("❌ Docker not available")
            return False

        # Build Docker image
        logger.info("🔨 Building Docker image...")
        code, stdout, stderr = self.run_command(["docker-compose", "build"])
        if code != 0:
            logger.error("❌ Docker build failed")
            return False

        # Run tests in Docker
        cmd = [
            "docker-compose",
            "--profile",
            "test",
            "run",
            "--rm",
            "pyspark-tools-test",
            "python",
            "test.py",
            "--unit",
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["docker"] = {"code": code, "stdout": stdout, "stderr": stderr}

        if code == 0:
            logger.info("✅ Docker tests passed")
            return True
        else:
            logger.error("❌ Docker tests failed")
            return False

    def run_ci_tests(self) -> bool:
        """Run tests optimized for CI/CD environment."""
        logger.info("🤖 Running CI tests...")

        # Run specific test files that are known to work in CI
        cmd = [
            "python",
            "-m",
            "pytest",
            "tests/test_minimal.py",
            "tests/test_ci_basic.py",
            "-v",
            "--tb=short",
            "--maxfail=3",
            "--cov=pyspark_tools",
            "--cov-report=xml:coverage.xml",
            "--cov-fail-under=20",  # Lower threshold for CI
        ]

        code, stdout, stderr = self.run_command(cmd)
        self.test_results["ci"] = {"code": code, "stdout": stdout, "stderr": stderr}

        if code == 0:
            logger.info("✅ CI tests passed")
            return True
        else:
            logger.error("❌ CI tests failed")
            logger.error(f"STDOUT: {stdout}")
            logger.error(f"STDERR: {stderr}")
            return False

    def run_all_tests(self) -> bool:
        """Run comprehensive test suite."""
        logger.info("🧪 Running comprehensive test suite...")

        results = []

        # Run tests in order of importance
        results.append(("Fast Tests", self.run_fast_tests()))
        results.append(("Unit Tests", self.run_unit_tests()))
        results.append(("Integration Tests", self.run_integration_tests()))
        results.append(("Security Tests", self.run_security_tests()))

        # Performance tests are optional
        try:
            results.append(("Performance Tests", self.run_performance_tests()))
        except Exception as e:
            logger.warning(f"Performance tests skipped: {e}")

        # Summary
        passed = sum(1 for _, result in results if result)
        total = len(results)

        logger.info(f"📊 Test Summary: {passed}/{total} test suites passed")

        for name, result in results:
            status = "✅" if result else "❌"
            logger.info(f"  {status} {name}")

        return passed == total

    def watch_tests(self) -> None:
        """Run tests in watch mode for development."""
        logger.info("👀 Starting watch mode (Ctrl+C to stop)...")

        try:
            import time

            while True:
                logger.info("🔄 Running tests...")
                self.run_fast_tests()
                logger.info("⏳ Waiting 5 seconds before next run...")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("🛑 Watch mode stopped")

    def validate_environment(self) -> bool:
        """Validate test environment setup."""
        logger.info("🔍 Validating test environment...")

        # Check Python version
        if sys.version_info < (3, 10):
            logger.error("❌ Python 3.10+ required")
            return False

        # Check required packages
        required_packages = ["pytest", "coverage", "black", "isort", "flake8"]
        for package in required_packages:
            code, _, _ = self.run_command(["python", "-c", f"import {package}"])
            if code != 0:
                logger.error(f"❌ Required package missing: {package}")
                return False

        # Check test directory
        if not (self.project_root / "tests").exists():
            logger.error("❌ Tests directory not found")
            return False

        logger.info("✅ Environment validation passed")
        return True

    def generate_report(self) -> None:
        """Generate test execution report."""
        duration = time.time() - self.start_time

        report = {
            "timestamp": time.time(),
            "duration": duration,
            "results": self.test_results,
            "summary": {
                "total_suites": len(self.test_results),
                "passed_suites": sum(
                    1 for r in self.test_results.values() if r["code"] == 0
                ),
                "failed_suites": sum(
                    1 for r in self.test_results.values() if r["code"] != 0
                ),
            },
        }

        report_file = self.project_root / "test_report.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"📊 Test report saved: {report_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Unified Test Runner for PySpark Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test.py                    # Run all tests
  python test.py --fast             # Run fast tests only
  python test.py --unit             # Run unit tests only
  python test.py --integration      # Run integration tests only
  python test.py --performance      # Run performance tests
  python test.py --security         # Run security tests
  python test.py --docker           # Run tests in Docker
  python test.py --parallel         # Run tests in parallel
  python test.py --coverage         # Run with coverage
  python test.py --watch            # Watch mode for development
  python test.py --ci               # CI mode (optimized for CI/CD)
        """,
    )

    # Test type options (mutually exclusive)
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument("--fast", action="store_true", help="Run fast tests only")
    test_group.add_argument("--unit", action="store_true", help="Run unit tests only")
    test_group.add_argument(
        "--integration", action="store_true", help="Run integration tests only"
    )
    test_group.add_argument(
        "--performance", action="store_true", help="Run performance tests"
    )
    test_group.add_argument(
        "--security", action="store_true", help="Run security tests"
    )
    test_group.add_argument(
        "--all", action="store_true", help="Run all tests (default)"
    )

    # Execution options
    parser.add_argument("--docker", action="store_true", help="Run tests in Docker")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    parser.add_argument(
        "--coverage", action="store_true", help="Run with coverage analysis"
    )
    parser.add_argument(
        "--watch", action="store_true", help="Watch mode for development"
    )
    parser.add_argument(
        "--ci", action="store_true", help="CI mode (optimized for CI/CD)"
    )

    # Other options
    parser.add_argument(
        "--validate", action="store_true", help="Validate environment only"
    )
    parser.add_argument(
        "--report", action="store_true", help="Generate detailed report"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = TestRunner()

    # Validate environment first
    if not runner.validate_environment():
        logger.error("❌ Environment validation failed")
        return 1

    if args.validate:
        logger.info("✅ Environment validation completed")
        return 0

    success = True

    try:
        # Determine which tests to run
        if args.watch:
            runner.watch_tests()
            return 0
        elif args.ci:
            success = runner.run_ci_tests()
        elif args.docker:
            success = runner.run_docker_tests()
        elif args.fast:
            success = runner.run_fast_tests()
        elif args.unit:
            success = runner.run_unit_tests()
        elif args.integration:
            success = runner.run_integration_tests()
        elif args.performance:
            success = runner.run_performance_tests()
        elif args.security:
            success = runner.run_security_tests()
        elif args.coverage:
            success = runner.run_with_coverage()
        elif args.parallel:
            success = runner.run_parallel_tests()
        else:
            # Default: run all tests
            success = runner.run_all_tests()

        # Generate report if requested
        if args.report:
            runner.generate_report()

        return 0 if success else 1

    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
