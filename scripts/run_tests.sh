#!/bin/bash

# =============================================================================
# E-Commerce Data Pipeline - Test Runner Script
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Virtual environment path
VENV_PATH="${PROJECT_ROOT}/venv"

# Print banner
print_banner() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║       E-Commerce Data Pipeline - Test Suite                   ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Log functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

# Setup virtual environment
setup_venv() {
    if [ ! -d "$VENV_PATH" ]; then
        log_info "Creating virtual environment..."
        python3 -m venv "$VENV_PATH"
    fi
    
    log_info "Activating virtual environment..."
    source "${VENV_PATH}/bin/activate"
    
    log_info "Installing dependencies..."
    pip install -q -r "${PROJECT_ROOT}/requirements.txt"
    pip install -q pytest pytest-cov pytest-mock pytest-asyncio
}

# Run unit tests
run_unit_tests() {
    log_step "Running Unit Tests..."
    
    cd "$PROJECT_ROOT"
    
    python -m pytest tests/ \
        -v \
        --tb=short \
        --cov=src \
        --cov-report=term-missing \
        --cov-report=html:coverage_report \
        --junitxml=test_results/unit_tests.xml \
        2>&1 | tee test_results/unit_tests.log
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -eq 0 ]; then
        log_info "Unit tests passed!"
    else
        log_error "Unit tests failed!"
    fi
    
    return $exit_code
}

# Run integration tests
run_integration_tests() {
    log_step "Running Integration Tests..."
    
    cd "$PROJECT_ROOT"
    
    # Check if services are running
    if ! docker-compose ps | grep -q "Up"; then
        log_warn "Services are not running. Starting services..."
        ./scripts/start_pipeline.sh start
        sleep 30
    fi
    
    python -m pytest tests/integration/ \
        -v \
        --tb=short \
        --junitxml=test_results/integration_tests.xml \
        2>&1 | tee test_results/integration_tests.log
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -eq 0 ]; then
        log_info "Integration tests passed!"
    else
        log_error "Integration tests failed!"
    fi
    
    return $exit_code
}

# Run data quality tests
run_data_quality_tests() {
    log_step "Running Data Quality Tests..."
    
    cd "$PROJECT_ROOT"
    
    python -m pytest tests/test_data_quality.py \
        -v \
        --tb=short \
        --junitxml=test_results/data_quality_tests.xml \
        2>&1 | tee test_results/data_quality_tests.log
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -eq 0 ]; then
        log_info "Data quality tests passed!"
    else
        log_error "Data quality tests failed!"
    fi
    
    return $exit_code
}

# Run linting
run_linting() {
    log_step "Running Code Linting..."
    
    cd "$PROJECT_ROOT"
    
    # Install linting tools
    pip install -q flake8 black isort mypy
    
    echo ""
    echo "Running flake8..."
    flake8 src/ --max-line-length=100 --ignore=E501,W503 || true
    
    echo ""
    echo "Running black (check mode)..."
    black src/ --check --diff || true
    
    echo ""
    echo "Running isort (check mode)..."
    isort src/ --check-only --diff || true
    
    echo ""
    echo "Running mypy..."
    mypy src/ --ignore-missing-imports || true
    
    log_info "Linting complete!"
}

# Run all tests
run_all_tests() {
    local failed=0
    
    mkdir -p "${PROJECT_ROOT}/test_results"
    
    run_unit_tests || failed=1
    run_data_quality_tests || failed=1
    run_linting
    
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}   Test Summary${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    
    if [ $failed -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Some tests failed!${NC}"
    fi
    
    echo ""
    echo "Test results saved to: ${PROJECT_ROOT}/test_results/"
    echo "Coverage report saved to: ${PROJECT_ROOT}/coverage_report/"
    echo ""
    
    return $failed
}

# Run specific test file
run_specific_test() {
    local test_file=$1
    
    log_step "Running specific test: $test_file"
    
    cd "$PROJECT_ROOT"
    
    python -m pytest "$test_file" -v --tb=long
}

# Show help
show_help() {
    echo ""
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  all           Run all tests (default)"
    echo "  unit          Run unit tests only"
    echo "  integration   Run integration tests only"
    echo "  quality       Run data quality tests only"
    echo "  lint          Run code linting only"
    echo "  file <path>   Run specific test file"
    echo "  help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                          # Run all tests"
    echo "  $0 unit                     # Run unit tests"
    echo "  $0 file tests/test_ingestion.py  # Run specific test"
    echo ""
}

# Main function
main() {
    print_banner
    
    local command=${1:-all}
    
    # Setup environment
    setup_venv
    
    # Create test results directory
    mkdir -p "${PROJECT_ROOT}/test_results"
    
    case $command in
        all)
            run_all_tests
            ;;
        unit)
            run_unit_tests
            ;;
        integration)
            run_integration_tests
            ;;
        quality)
            run_data_quality_tests
            ;;
        lint)
            run_linting
            ;;
        file)
            if [ -z "$2" ]; then
                log_error "Please specify a test file"
                exit 1
            fi
            run_specific_test "$2"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Run main
main "$@"