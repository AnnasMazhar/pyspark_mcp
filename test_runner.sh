#!/bin/bash

# PySpark Tools - Comprehensive Docker-based Testing Script
# This script provides various testing options using Docker containers

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to build the Docker image
build_image() {
    print_status "Building Docker image..."
    if docker-compose build; then
        print_success "Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Function to build the Docker image (clean)
build_image_clean() {
    print_status "Building Docker image (no cache)..."
    if docker-compose build --no-cache; then
        print_success "Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Function to run all tests
run_all_tests() {
    print_status "Running comprehensive test suite..."
    docker-compose --profile test run --rm pyspark-tools-test
}

# Function to run module-specific tests
run_module_tests() {
    local module=$1
    print_status "Running tests for module: $module"
    
    case $module in
        "sql-converter")
            docker-compose --profile test run --rm test-sql-converter
            ;;
        "batch-processor")
            docker-compose --profile test run --rm test-batch-processor
            ;;
        "duplicate-detector")
            docker-compose --profile test run --rm test-duplicate-detector
            ;;
        "file-utils")
            docker-compose --profile test run --rm test-file-utils
            ;;
        "server")
            docker-compose --profile test run --rm test-server
            ;;
        *)
            print_error "Unknown module: $module"
            print_status "Available modules: sql-converter, batch-processor, duplicate-detector, file-utils, server"
            exit 1
            ;;
    esac
}

# Function to run performance tests
run_performance_tests() {
    print_status "Running performance tests..."
    docker-compose --profile performance run --rm test-performance
}

# Function to run integration tests
run_integration_tests() {
    print_status "Running integration tests..."
    docker-compose --profile integration run --rm test-integration
}

# Function to run tests with coverage
run_coverage_tests() {
    print_status "Running tests with coverage analysis..."
    
    # Create coverage reports directory
    mkdir -p coverage_reports
    
    # Run tests with coverage
    docker-compose --profile test run --rm pyspark-tools-test
    
    print_success "Coverage report generated in coverage_reports/ directory"
    print_status "Open coverage_reports/index.html in your browser to view detailed coverage"
}

# Function to run quality checks only
run_quality_checks() {
    print_status "Running code quality checks..."
    
    # Build image (which includes quality checks)
    build_image
    
    print_success "All quality checks passed"
}

# Function to run specific test file
run_specific_test() {
    local test_file=$1
    print_status "Running specific test file: $test_file"
    
    docker-compose --profile test run --rm pyspark-tools-test pytest "tests/$test_file" -v --tb=short
}

# Function to run tests in watch mode (for development)
run_watch_mode() {
    print_status "Running tests in watch mode (press Ctrl+C to stop)..."
    print_warning "This will rebuild and test on file changes"
    
    while true; do
        print_status "Running test cycle..."
        docker-compose --profile test run --rm pyspark-tools-test || true
        print_status "Waiting 10 seconds before next cycle..."
        sleep 10
    done
}

# Function to cleanup Docker resources
cleanup() {
    print_status "Cleaning up Docker resources..."
    docker-compose --profile test down --volumes --remove-orphans
    docker system prune -f
    print_success "Cleanup completed"
}

# Function to show usage
show_usage() {
    echo "PySpark Tools - Docker Testing Script"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  all                    Run all tests with coverage"
    echo "  module <name>          Run tests for specific module"
    echo "  performance           Run performance tests"
    echo "  integration           Run integration tests"
    echo "  coverage              Run tests with detailed coverage analysis"
    echo "  quality               Run code quality checks only"
    echo "  specific <file>       Run specific test file"
    echo "  watch                 Run tests in watch mode (development)"
    echo "  build                 Build Docker image"
    echo "  cleanup               Clean up Docker resources"
    echo "  help                  Show this help message"
    echo ""
    echo "Module names:"
    echo "  sql-converter         SQL to PySpark conversion tests"
    echo "  batch-processor       Batch processing tests"
    echo "  duplicate-detector    Duplicate code detection tests"
    echo "  file-utils           File utilities tests"
    echo "  server               MCP server tests"
    echo ""
    echo "Examples:"
    echo "  $0 all                           # Run all tests"
    echo "  $0 module sql-converter          # Test SQL converter module"
    echo "  $0 specific test_sql_converter.py # Run specific test file"
    echo "  $0 coverage                      # Run with coverage analysis"
    echo "  $0 build                         # Build Docker image"
}

# Main script logic
main() {
    check_docker
    
    case "${1:-help}" in
        "all")
            build_image
            run_coverage_tests
            ;;
        "quick")
            # Skip build for quick tests
            print_status "Running quick tests (no rebuild)..."
            docker-compose --profile test run --rm pyspark-tools-test pytest tests/ -v --tb=short -x
            ;;
        "module")
            if [ -z "$2" ]; then
                print_error "Module name required"
                show_usage
                exit 1
            fi
            build_image
            run_module_tests "$2"
            ;;
        "performance")
            build_image
            run_performance_tests
            ;;
        "integration")
            build_image
            run_integration_tests
            ;;
        "coverage")
            build_image
            run_coverage_tests
            ;;
        "quality")
            run_quality_checks
            ;;
        "specific")
            if [ -z "$2" ]; then
                print_error "Test file name required"
                show_usage
                exit 1
            fi
            build_image
            run_specific_test "$2"
            ;;
        "watch")
            build_image
            run_watch_mode
            ;;
        "build")
            build_image
            ;;
        "build-clean")
            build_image_clean
            ;;
        "cleanup")
            cleanup
            ;;
        "help"|*)
            show_usage
            ;;
    esac
}

# Run main function with all arguments
main "$@"