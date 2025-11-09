# PySpark Tools - Docker-based Testing Makefile
# This Makefile provides convenient commands for running tests in Docker

.PHONY: help build test test-all test-module test-coverage test-performance test-integration
.PHONY: test-sql-converter test-batch-processor test-duplicate-detector test-file-utils test-server
.PHONY: quality lint format clean cleanup watch

# Default target
help: ## Show this help message
	@echo "PySpark Tools - Docker Testing Commands"
	@echo ""
	@echo "Usage: make [TARGET]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build targets
build: ## Build Docker image (cached)
	@echo "🔨 Building Docker image..."
	docker-compose build

build-clean: ## Build Docker image with no cache (for clean builds)
	@echo "🔨 Building Docker image (no cache)..."
	docker-compose build --no-cache

# Test targets
test: test-quick ## Run quick tests (alias for test-quick)

test-all: build ## Run comprehensive test suite with coverage
	@echo "🧪 Running all tests with coverage..."
	./test_runner.sh all

test-quick: ## Run quick tests without rebuild (FAST)
	@echo "⚡ Running quick tests..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/test_advanced_optimizer.py -v --tb=short

test-coverage: build ## Run tests with detailed coverage analysis
	@echo "📊 Running coverage analysis..."
	./test_runner.sh coverage

# Module-specific testing
test-module: ## Run tests for specific module (usage: make test-module MODULE=sql-converter)
	@if [ -z "$(MODULE)" ]; then \
		echo "❌ MODULE parameter required. Usage: make test-module MODULE=sql-converter"; \
		echo "Available modules: sql-converter, batch-processor, duplicate-detector, file-utils, server"; \
		exit 1; \
	fi
	@echo "🔬 Testing module: $(MODULE)"
	./test_runner.sh module $(MODULE)

test-sql-converter: ## Run SQL converter module tests (FAST)
	@echo "🔬 Testing SQL converter module..."
	docker-compose --profile test run --rm test-sql-converter

test-batch-processor: ## Run batch processor module tests (FAST)
	@echo "🔬 Testing batch processor module..."
	docker-compose --profile test run --rm test-batch-processor

test-duplicate-detector: ## Run duplicate detector module tests (FAST)
	@echo "🔬 Testing duplicate detector module..."
	docker-compose --profile test run --rm test-duplicate-detector

test-file-utils: ## Run file utilities module tests (FAST)
	@echo "🔬 Testing file utilities module..."
	docker-compose --profile test run --rm test-file-utils

test-server: ## Run MCP server module tests (FAST)
	@echo "🔬 Testing MCP server module..."
	docker-compose --profile test run --rm test-server

test-advanced-optimizer: ## Run advanced optimizer module tests (FAST)
	@echo "🔬 Testing advanced optimizer module..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/test_advanced_optimizer.py -v --tb=short

test-data-source-analyzer: ## Run data source analyzer module tests (FAST)
	@echo "🔬 Testing data source analyzer module..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/test_data_source_analyzer.py -v --tb=short

# Specialized testing
test-performance: build ## Run performance tests
	@echo "⚡ Running performance tests..."
	./test_runner.sh performance

benchmark: build ## Run comprehensive performance benchmarks
	@echo "🎯 Running performance benchmarks..."
	docker-compose --profile test run --rm pyspark-tools-test python scripts/benchmark_performance.py

validate-performance: build ## Validate performance against v1.0 targets
	@echo "✅ Validating performance targets..."
	docker-compose --profile test run --rm pyspark-tools-test bash scripts/validate_performance.sh

test-integration: build ## Run integration tests
	@echo "🔗 Running integration tests..."
	./test_runner.sh integration

test-specific: ## Run specific test file (usage: make test-specific FILE=test_sql_converter.py)
	@if [ -z "$(FILE)" ]; then \
		echo "❌ FILE parameter required. Usage: make test-specific FILE=test_sql_converter.py"; \
		exit 1; \
	fi
	@echo "🎯 Running specific test: $(FILE)"
	./test_runner.sh specific $(FILE)

# Quality targets
quality: build ## Run code quality checks only
	@echo "✨ Running quality checks..."
	./test_runner.sh quality

lint: ## Run linting checks in Docker
	@echo "🔍 Running linting..."
	docker-compose run --rm pyspark-tools-test flake8 pyspark_tools/ tests/ run_server.py

format: ## Format code using black and isort
	@echo "🎨 Formatting code..."
	docker-compose run --rm pyspark-tools-test python -m black pyspark_tools/ tests/ run_server.py
	docker-compose run --rm pyspark-tools-test python -m isort pyspark_tools/ tests/ run_server.py

# Development targets
watch: build ## Run tests in watch mode (for development)
	@echo "👀 Starting watch mode..."
	./test_runner.sh watch

# Utility targets
clean: ## Clean up Docker resources
	@echo "🧹 Cleaning up..."
	./test_runner.sh cleanup

cleanup: clean ## Alias for clean

# Docker compose shortcuts
up: ## Start the main application service
	@echo "🚀 Starting PySpark Tools server..."
	docker-compose up -d pyspark-tools

down: ## Stop all services
	@echo "🛑 Stopping services..."
	docker-compose down

logs: ## Show logs from the main service
	@echo "📋 Showing logs..."
	docker-compose logs -f pyspark-tools

# Advanced testing combinations
test-no-build: ## Run tests without rebuilding (FASTEST)
	@echo "⚡ Running tests without rebuild..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v --tb=short -x

test-slow: ## Run slow tests (integration and performance)
	@echo "🐌 Running slow tests..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -m "integration or performance" --tb=short

test-unit: ## Run unit tests only
	@echo "🔬 Running unit tests..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -m "unit" --tb=short

# Parallel testing
test-parallel: build ## Run tests in parallel (faster execution)
	@echo "⚡ Running tests in parallel..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -n auto --tb=short

# Coverage targets
coverage-html: build ## Generate HTML coverage report
	@echo "📊 Generating HTML coverage report..."
	mkdir -p coverage_reports
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ --cov=pyspark_tools --cov-report=html:/app/coverage_html
	@echo "✅ Coverage report available in coverage_reports/index.html"

coverage-xml: build ## Generate XML coverage report (for CI)
	@echo "📊 Generating XML coverage report..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ --cov=pyspark_tools --cov-report=xml:/app/coverage.xml

# CI/CD targets
ci-test: ## Run tests suitable for CI environment
	@echo "🤖 Running CI tests..."
	docker-compose build --no-cache
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v --cov=pyspark_tools --cov-report=term --cov-report=xml --tb=short

# Development helpers
shell: ## Open shell in test container
	@echo "🐚 Opening shell in test container..."
	docker-compose --profile test run --rm pyspark-tools-test bash

debug: ## Run tests with debugging enabled
	@echo "🐛 Running tests with debugging..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -s --tb=long --pdb

# Documentation
test-docs: ## Test documentation examples
	@echo "📚 Testing documentation examples..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -k "doc" --tb=short

# Security testing
test-security: ## Run security-focused tests
	@echo "🔒 Running security tests..."
	docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v -k "security" --tb=short

security-audit: build ## Run comprehensive security audit
	@echo "🔐 Running security audit..."
	docker-compose --profile test run --rm pyspark-tools-test python scripts/security_audit.py

security-scan: ## Run dependency vulnerability scan
	@echo "🔍 Scanning for vulnerabilities..."
	docker-compose --profile test run --rm pyspark-tools-test python -c "import subprocess; subprocess.run(['python', '-m', 'pip', 'install', 'safety'], check=False); subprocess.run(['safety', 'check', '--file', 'requirements.txt'])" || echo "⚠️  Safety scan completed with warnings"

# Release targets
tag-release: ## Tag a new release (usage: make tag-release VERSION=1.0.0)
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ VERSION parameter required. Usage: make tag-release VERSION=1.0.0"; \
		exit 1; \
	fi
	@echo "🏷️  Tagging release v$(VERSION)..."
	./scripts/tag_release.sh $(VERSION)

prepare-release: ## Prepare release (run all quality checks)
	@echo "🚀 Preparing release..."
	make build-clean
	make test-all
	make test-security
	make test-performance
	make coverage-html
	@echo "✅ Release preparation complete!"

validate-release: ## Validate release readiness
	@echo "✅ Validating release readiness..."
	@echo "📋 Checking required files..."
	@test -f CHANGELOG.md || (echo "❌ CHANGELOG.md missing" && exit 1)
	@test -f LICENSE || (echo "❌ LICENSE missing" && exit 1)
	@test -f README.md || (echo "❌ README.md missing" && exit 1)
	@test -f pyproject.toml || (echo "❌ pyproject.toml missing" && exit 1)
	@test -f requirements.txt || (echo "❌ requirements.txt missing" && exit 1)
	@test -f Dockerfile || (echo "❌ Dockerfile missing" && exit 1)
	@echo "📊 Checking version consistency..."
	@grep -q "version = " pyproject.toml || (echo "❌ Version not found in pyproject.toml" && exit 1)
	@echo "🧪 Running final quality checks..."
	make lint
	make test-all
	@echo "✅ Release validation passed!"

publish-pypi: ## Publish to PyPI (requires authentication)
	@echo "📦 Publishing to PyPI..."
	docker-compose run --rm pyspark-tools-test python -m build
	docker-compose run --rm pyspark-tools-test python -m twine upload dist/*

docker-push: ## Push Docker image to registry
	@echo "🐳 Pushing Docker image..."
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ VERSION parameter required. Usage: make docker-push VERSION=1.0.0"; \
		exit 1; \
	fi
	docker tag pyspark-tools:latest pyspark-tools:$(VERSION)
	docker push pyspark-tools:$(VERSION)
	docker push pyspark-tools:latest

validate-production: ## Validate production deployment readiness
	@echo "🏭 Validating production deployment readiness..."
	@echo "📋 This will run comprehensive validation including:"
	@echo "   • Docker build and container startup"
	@echo "   • MCP endpoint functionality"
	@echo "   • Real-world usage scenarios"
	@echo "   • Resource usage under load"
	@echo "   • Configuration security"
	@echo ""
	@read -p "Continue with production validation? [y/N] " confirm && [ "$$confirm" = "y" ]
	python scripts/validate_production_deployment.py

production-ready: ## Complete production readiness validation
	@echo "🎯 Running complete production readiness validation..."
	make validate-release
	make validate-performance
	make security-audit
	make validate-production
	@echo ""
	@echo "✅ Production readiness validation complete!"
	@echo "📋 Review PRODUCTION_READINESS.md for final checklist"