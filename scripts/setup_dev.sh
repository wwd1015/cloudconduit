#!/bin/bash
# Setup CloudConduit development environment

set -e

echo "🚀 Setting up CloudConduit development environment..."

# Change to project root
cd "$(dirname "$0")/.."

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1-2)
REQUIRED_VERSION="3.12"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)" 2>/dev/null; then
    echo "❌ Python 3.12 or higher is required. Found: Python $PYTHON_VERSION"
    echo "   Please install Python 3.12+ and try again"
    exit 1
fi

echo "✅ Python version check passed: Python $PYTHON_VERSION"

# Create virtual environment if it doesn't exist
if [[ ! -d "venv" ]]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install package in development mode with all dependencies
echo "📚 Installing CloudConduit in development mode..."
pip install -e ".[all]"

echo "✅ Development dependencies installed"

# Install pre-commit hooks (optional)
if command -v pre-commit >/dev/null 2>&1 || pip show pre-commit >/dev/null 2>&1; then
    echo "🪝 Setting up pre-commit hooks..."
    pip install pre-commit
    pre-commit install
    echo "✅ Pre-commit hooks installed"
fi

# Run initial tests
echo "🧪 Running initial test suite..."
python -m pytest tests/unit/ -x --tb=short

# Display setup summary
echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Activate the environment: source venv/bin/activate"
echo "   2. Configure credentials (see README.md)"
echo "   3. Run tests: ./scripts/run_tests.sh"
echo "   4. Build docs: ./scripts/build_docs.sh"
echo ""
echo "💡 Useful commands:"
echo "   pytest tests/unit/          # Run unit tests"
echo "   pytest tests/integration/   # Run integration tests (needs credentials)"
echo "   black cloudconduit/         # Format code"
echo "   mypy cloudconduit/          # Type checking"
echo "   sphinx-build docs/source docs/build/html  # Build documentation"
echo ""
echo "📖 See README.md for detailed usage instructions"