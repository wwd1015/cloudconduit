#!/bin/bash
# Run CloudConduit test suite

set -e

echo "🧪 Running CloudConduit test suite..."

# Change to project root
cd "$(dirname "$0")/.."

# Default test options
UNIT_TESTS=true
INTEGRATION_TESTS=false
COVERAGE=true
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --integration)
            INTEGRATION_TESTS=true
            shift
            ;;
        --no-unit)
            UNIT_TESTS=false
            shift
            ;;
        --no-coverage)
            COVERAGE=false
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --integration    Run integration tests (requires credentials)"
            echo "  --no-unit        Skip unit tests"
            echo "  --no-coverage    Skip coverage reporting"
            echo "  --verbose, -v    Verbose output"
            echo "  --help, -h       Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for available options"
            exit 1
            ;;
    esac
done

# Build pytest command
PYTEST_ARGS=""

if [[ "$VERBOSE" == "true" ]]; then
    PYTEST_ARGS="$PYTEST_ARGS -v"
fi

if [[ "$COVERAGE" == "true" ]]; then
    PYTEST_ARGS="$PYTEST_ARGS --cov=cloudconduit --cov-report=term-missing --cov-report=html --cov-branch"
fi

# Run unit tests
if [[ "$UNIT_TESTS" == "true" ]]; then
    echo "🔬 Running unit tests..."
    python -m pytest tests/unit/ -m "not integration" $PYTEST_ARGS
    echo "✅ Unit tests completed"
fi

# Run integration tests if requested
if [[ "$INTEGRATION_TESTS" == "true" ]]; then
    echo "🌐 Running integration tests..."
    echo "⚠️  Note: Integration tests require valid credentials"
    python -m pytest tests/integration/ -m "integration" $PYTEST_ARGS --tb=short
    echo "✅ Integration tests completed"
fi

# Show coverage report location
if [[ "$COVERAGE" == "true" ]]; then
    echo "📊 Coverage report generated:"
    echo "   HTML: htmlcov/index.html"
    echo "   Terminal output above"
    
    # Optionally open coverage report (macOS)
    if [[ "$OSTYPE" == "darwin"* ]] && command -v open >/dev/null 2>&1 && [[ -f "htmlcov/index.html" ]]; then
        read -p "Open coverage report in browser? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            open htmlcov/index.html
        fi
    fi
fi

echo "🎉 Test suite completed successfully!"