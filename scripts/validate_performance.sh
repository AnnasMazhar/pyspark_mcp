#!/bin/bash

# Performance Validation Script for PySpark Tools
# This script runs performance benchmarks and validates against v1.0 targets

set -e

echo "🎯 PySpark Tools v1.0 Performance Validation"
echo "============================================="

# Performance targets
STARTUP_TARGET=5.0
CONVERSION_TARGET=2.0
MEMORY_TARGET=512
SUCCESS_RATE_TARGET=95.0
ERROR_RATE_TARGET=1.0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✅ PASS${NC}: $message"
    elif [ "$status" = "FAIL" ]; then
        echo -e "${RED}❌ FAIL${NC}: $message"
    else
        echo -e "${YELLOW}ℹ️  INFO${NC}: $message"
    fi
}

# Check if running in Docker
if [ -f /.dockerenv ]; then
    echo "🐳 Running in Docker environment"
    PYTHON_CMD="python"
else
    echo "💻 Running in local environment"
    PYTHON_CMD="python3"
fi

# Ensure we're in the right directory
cd "$(dirname "$0")/.."

# Install required packages if not available
echo "📦 Checking dependencies..."
$PYTHON_CMD -c "import psutil" 2>/dev/null || {
    echo "Installing psutil..."
    pip install psutil
}

# Run the performance benchmark
echo ""
echo "🚀 Running performance benchmarks..."
echo "This may take several minutes..."
echo ""

if $PYTHON_CMD scripts/benchmark_performance.py; then
    BENCHMARK_EXIT_CODE=0
else
    BENCHMARK_EXIT_CODE=1
fi

# Parse results if available
RESULTS_FILE="benchmark_results.json"
if [ -f "$RESULTS_FILE" ]; then
    echo ""
    echo "📊 Analyzing benchmark results..."
    
    # Extract key metrics using Python
    METRICS=$($PYTHON_CMD -c "
import json
import sys

try:
    with open('$RESULTS_FILE', 'r') as f:
        data = json.load(f)
    
    results = data['results']
    
    # Extract metrics
    startup_time = results.get('server_startup', {}).get('startup_time_seconds', 999)
    conversion_time = results.get('sql_conversion_speed', {}).get('max_conversion_time_seconds', 999)
    memory_usage = results.get('memory_efficiency', {}).get('memory_usage_mb', 999)
    success_rate = results.get('sql_conversion_speed', {}).get('success_rate_percent', 0)
    error_rate = results.get('error_rate', {}).get('error_rate_percent', 100)
    batch_files = results.get('batch_processing', {}).get('concurrent_150_success_rate', 0)
    
    print(f'{startup_time},{conversion_time},{memory_usage},{success_rate},{error_rate},{batch_files}')
    
except Exception as e:
    print('999,999,999,0,100,0')
    sys.exit(1)
")
    
    IFS=',' read -r STARTUP_TIME CONVERSION_TIME MEMORY_USAGE SUCCESS_RATE ERROR_RATE BATCH_SUCCESS <<< "$METRICS"
    
    echo ""
    echo "📋 Performance Validation Results:"
    echo "=================================="
    
    # Validate each metric
    VALIDATION_PASSED=true
    
    # Startup time validation
    if (( $(echo "$STARTUP_TIME < $STARTUP_TARGET" | bc -l) )); then
        print_status "PASS" "Server startup time: ${STARTUP_TIME}s (target: <${STARTUP_TARGET}s)"
    else
        print_status "FAIL" "Server startup time: ${STARTUP_TIME}s (target: <${STARTUP_TARGET}s)"
        VALIDATION_PASSED=false
    fi
    
    # Conversion speed validation
    if (( $(echo "$CONVERSION_TIME < $CONVERSION_TARGET" | bc -l) )); then
        print_status "PASS" "SQL conversion speed: ${CONVERSION_TIME}s (target: <${CONVERSION_TARGET}s)"
    else
        print_status "FAIL" "SQL conversion speed: ${CONVERSION_TIME}s (target: <${CONVERSION_TARGET}s)"
        VALIDATION_PASSED=false
    fi
    
    # Memory usage validation
    if (( $(echo "$MEMORY_USAGE < $MEMORY_TARGET" | bc -l) )); then
        print_status "PASS" "Memory usage: ${MEMORY_USAGE}MB (target: <${MEMORY_TARGET}MB)"
    else
        print_status "FAIL" "Memory usage: ${MEMORY_USAGE}MB (target: <${MEMORY_TARGET}MB)"
        VALIDATION_PASSED=false
    fi
    
    # Success rate validation
    if (( $(echo "$SUCCESS_RATE >= $SUCCESS_RATE_TARGET" | bc -l) )); then
        print_status "PASS" "Conversion success rate: ${SUCCESS_RATE}% (target: >${SUCCESS_RATE_TARGET}%)"
    else
        print_status "FAIL" "Conversion success rate: ${SUCCESS_RATE}% (target: >${SUCCESS_RATE_TARGET}%)"
        VALIDATION_PASSED=false
    fi
    
    # Error rate validation
    if (( $(echo "$ERROR_RATE < $ERROR_RATE_TARGET" | bc -l) )); then
        print_status "PASS" "Error rate: ${ERROR_RATE}% (target: <${ERROR_RATE_TARGET}%)"
    else
        print_status "FAIL" "Error rate: ${ERROR_RATE}% (target: <${ERROR_RATE_TARGET}%)"
        VALIDATION_PASSED=false
    fi
    
    # Batch processing validation
    if (( $(echo "$BATCH_SUCCESS >= 90" | bc -l) )); then
        print_status "PASS" "Batch processing: ${BATCH_SUCCESS}% success rate (target: >90%)"
    else
        print_status "FAIL" "Batch processing: ${BATCH_SUCCESS}% success rate (target: >90%)"
        VALIDATION_PASSED=false
    fi
    
    echo ""
    echo "=================================="
    
    if [ "$VALIDATION_PASSED" = true ] && [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
        print_status "PASS" "All performance targets met! ✨"
        echo ""
        echo "🎉 PySpark Tools v1.0 is ready for release!"
        echo ""
        echo "📊 Performance Summary:"
        echo "  • Startup Time: ${STARTUP_TIME}s"
        echo "  • Conversion Speed: ${CONVERSION_TIME}s"
        echo "  • Memory Usage: ${MEMORY_USAGE}MB"
        echo "  • Success Rate: ${SUCCESS_RATE}%"
        echo "  • Error Rate: ${ERROR_RATE}%"
        echo "  • Batch Success: ${BATCH_SUCCESS}%"
        
        exit 0
    else
        print_status "FAIL" "Some performance targets not met"
        echo ""
        echo "❌ Performance validation failed. Please review and optimize before release."
        exit 1
    fi
    
else
    echo "❌ Benchmark results file not found"
    exit 1
fi