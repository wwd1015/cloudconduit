#!/bin/bash
# Build documentation using Sphinx

set -e

echo "🏗️  Building CloudConduit documentation..."

# Change to docs directory
cd "$(dirname "$0")/../docs"

# Clean previous builds
echo "🧹 Cleaning previous builds..."
make clean

# Build HTML documentation
echo "📖 Building HTML documentation..."
make html

# Check for broken links
echo "🔗 Checking for broken links..."
make linkcheck || echo "⚠️  Some links may be broken, check the output above"

echo "✅ Documentation built successfully!"
echo "📂 Open docs/build/html/index.html in your browser to view the documentation"

# Optionally open in browser (macOS)
if [[ "$OSTYPE" == "darwin"* ]] && command -v open >/dev/null 2>&1; then
    read -p "Open documentation in browser? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        open build/html/index.html
    fi
fi