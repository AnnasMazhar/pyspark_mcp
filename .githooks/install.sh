#!/bin/bash
# Install git hooks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_DIR="$(git rev-parse --git-dir)"

echo "Installing git hooks..."

# Make hooks executable
chmod +x "$SCRIPT_DIR/pre-push"

# Create symlink or copy hook
if [ -d "$GIT_DIR/hooks" ]; then
    ln -sf "$SCRIPT_DIR/pre-push" "$GIT_DIR/hooks/pre-push"
    echo "✅ Pre-push hook installed"
else
    echo "❌ Git hooks directory not found"
    exit 1
fi

echo ""
echo "Git hooks installed successfully!"
echo "To bypass hooks, use: git push --no-verify"
