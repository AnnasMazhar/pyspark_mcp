# PySpark Tools - Unified Testing Makefile
# Simple Makefile that uses the unified test.py entry point

.PHONY: help test build clean

# Default target
help: ## Show this help message
	@echo "PySpark Tools - Unified Testing"
	@echo ""
	@echo "Usage: make [TARGET]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $1, $2}' $(MAKEFILE_LIST)

# Main test targets using unified test runner
test: ## Run all tests (default)
	python test.py

test-fast: ## Run fast tests only
	python test.py --fast

test-unit: ## Run unit tests only
	python test.py --unit

test-integration: ## Run integration tests only
	python test.py --integration

test-performance: ## Run performance tests
	python test.py --performance

test-security: ## Run security tests
	python test.py --security

test-coverage: ## Run tests with coverage
	python test.py --coverage

test-parallel: ## Run tests in parallel
	python test.py --parallel

test-ci: ## Run CI-optimized tests
	python test.py --ci

test-watch: ## Run tests in watch mode
	python test.py --watch

# Docker targets
test-docker: build ## Run tests in Docker
	python test.py --docker

build: ## Build Docker image
	@echo "🔨 Building Docker image..."
	docker-compose build

build-clean: ## Build Docker image with no cache
	@echo "🔨 Building Docker image (no cache)..."
	docker-compose build --no-cache

# Docker shortcuts using unified test runner
docker-fast: build ## Run fast tests in Docker
	docker-compose --profile test run --rm test-fast

docker-coverage: build ## Run coverage tests in Docker
	docker-compose --profile test run --rm test-coverage

docker-performance: build ## Run performance tests in Docker
	docker-compose --profile test run --rm test-performance

docker-security: build ## Run security tests in Docker
	docker-compose --profile test run --rm test-security

# Development targets
validate: ## Validate test environment
	python test.py --validate

report: ## Generate test report
	python test.py --report

# Application targets
up: ## Start the main application
	@echo "🚀 Starting PySpark Tools server..."
	docker-compose up -d pyspark-tools

down: ## Stop all services
	@echo "🛑 Stopping services..."
	docker-compose down

logs: ## Show application logs
	@echo "📋 Showing logs..."
	docker-compose logs -f pyspark-tools

# Utility targets
clean: ## Clean up Docker resources
	@echo "🧹 Cleaning up..."
	docker-compose down --volumes --remove-orphans
	docker system prune -f

shell: ## Open shell in test container
	@echo "🐚 Opening shell..."
	docker-compose --profile test run --rm pyspark-tools-test bash

# Quality targets (using test.py internally)
lint: ## Run linting
	@echo "🔍 Running linting..."
	python -m flake8 pyspark_tools/ tests/ run_server.py

format: ## Format code
	@echo "🎨 Formatting code..."
	python -m black pyspark_tools/ tests/ run_server.py
	python -m isort pyspark_tools/ tests/ run_server.py

# Release targets
release-check: ## Check release readiness
	@echo "✅ Checking release readiness..."
	python test.py --ci
	python test.py --security
	python test.py --performance

# Help for common workflows
dev: ## Development workflow (fast tests + watch)
	@echo "� Sttarting development workflow..."
	@echo "Running fast tests first..."
	python test.py --fast
	@echo "Starting watch mode (Ctrl+C to stop)..."
	python test.py --watch

ci: ## CI workflow (what runs in CI/CD)
	python test.py --ci