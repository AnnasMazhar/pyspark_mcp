#!/bin/bash

# PySpark Tools Release Tagging Script
# Usage: ./scripts/tag_release.sh <version>
# Version is required, or defaults to the version in pyproject.toml.

set -euo pipefail

PYPROJECT_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

if [ "${1:-}" = "" ]; then
    VERSION="$PYPROJECT_VERSION"
    echo "No version argument given; using pyproject.toml version ${VERSION}"
else
    VERSION="$1"
fi

RELEASE_BRANCH="main"

echo "Preparing to tag release v${VERSION}"

# Verify we're on the correct branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "$RELEASE_BRANCH" ]; then
    echo "Error: Must be on $RELEASE_BRANCH branch to tag release"
    echo "   Current branch: $CURRENT_BRANCH"
    exit 1
fi

# Verify working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory is not clean"
    echo "   Please commit or stash changes before tagging"
    git status --short
    exit 1
fi

# Verify version in pyproject.toml matches
if [ "$PYPROJECT_VERSION" != "$VERSION" ]; then
    echo "Error: Version mismatch"
    echo "   pyproject.toml version: $PYPROJECT_VERSION"
    echo "   Requested version: $VERSION"
    exit 1
fi

# Run quality checks
echo "Running quality checks..."
make lint || {
    echo "Linting failed"
    exit 1
}

echo "Running tests..."
make test || {
    echo "Tests failed"
    exit 1
}

# Verify all release files exist
REQUIRED_FILES=(
    "CHANGELOG.md"
    "README.md"
    "LICENSE"
    "pyproject.toml"
    "Dockerfile"
    "docker-compose.yml"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Error: Required file missing: $file"
        exit 1
    fi
done

# Create git tag
echo "Creating git tag v${VERSION}..."
if [ -f "RELEASE_NOTES_v${VERSION}.md" ]; then
    git tag -a "v${VERSION}" -m "Release v${VERSION}

$(cat RELEASE_NOTES_v${VERSION}.md)"
else
    git tag -a "v${VERSION}" -m "Release v${VERSION}"
fi

echo "Release v${VERSION} tagged successfully."
echo ""
echo "Next steps:"
echo "   1. Push the tag: git push origin v${VERSION}"
echo "   2. Create GitHub release"
echo "   3. Publish to PyPI"
