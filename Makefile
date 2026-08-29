# PySpark Tools - Testing Makefile

.PHONY: help test test-fast test-unit test-coverage lint format build clean

help: ## Show this help message
	@echo "PySpark Tools"
	@echo ""
	@echo "Usage: make [TARGET]"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run the pytest suite
	python -m pytest tests/

test-fast: ## Run fast tests only
	python -m pytest tests/ -m fast

test-unit: ## Run unit tests only
	python -m pytest tests/ -m unit

test-integration: ## Run integration tests only
	python -m pytest tests/ -m integration

test-coverage: ## Run tests with coverage
	python -m pytest tests/ --cov=pyspark_tools --cov-fail-under=60

# Docker targets
build: ## Build Docker image
	@echo "Building Docker image..."
	docker compose build

up: ## Start the main application
	docker compose up -d pyspark-tools

down: ## Stop all services
	docker compose down

logs: ## Show application logs
	docker compose logs -f pyspark-tools

clean: ## Clean up Docker resources
	docker compose down --volumes --remove-orphans

lint: ## Run linting
	python -m flake8 pyspark_tools/ tests/ run_server.py

format: ## Format code
	python -m black pyspark_tools/ tests/ run_server.py
	python -m isort pyspark_tools/ tests/ run_server.py
