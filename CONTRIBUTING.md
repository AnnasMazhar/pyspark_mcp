# Contributing to PySpark MCP Tools

Thank you for your interest in contributing! This project follows trunk-based development practices with strict quality gates.

## Development Workflow

### 1. Branch Strategy

We use trunk-based development with short-lived feature branches:

- `main` - Production-ready code, protected branch
- `develop` - Integration branch for features (optional)
- `feature/*` - New features (e.g., `feature/add-snowflake-support`)
- `bugfix/*` - Bug fixes (e.g., `bugfix/fix-oracle-decode`)
- `hotfix/*` - Critical production fixes (e.g., `hotfix/security-patch`)
- `docs/*` - Documentation updates (e.g., `docs/update-readme`)

### 2. Getting Started

```bash
# Clone the repository
git clone https://github.com/your-org/pyspark_mcp.git
cd pyspark_mcp

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .

# Install development tools
pip install black isort flake8 pytest pytest-cov
```

### 3. Creating a Feature Branch

```bash
# Update main branch
git checkout main
git pull origin main

# Create feature branch
git checkout -b feature/my-new-feature

# Make your changes
# ... edit files ...

# Run tests locally
pytest tests/

# Format code
black pyspark_tools tests
isort pyspark_tools tests

# Commit changes
git add .
git commit -m "feat: add my new feature"

# Push to remote
git push origin feature/my-new-feature
```

### 4. Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `ci`: CI/CD changes
- `build`: Build system changes

**Examples:**
```
feat(sql-converter): add Snowflake dialect support

Add support for Snowflake-specific SQL functions including
DATEADD, DATEDIFF, and LISTAGG conversions to PySpark.

Closes #123
```

```
fix(oracle): resolve DECODE function conversion bug

Fixed issue where DECODE with odd number of arguments
was not handling default value correctly.

Fixes #456
```

### 5. Code Quality Standards

#### Formatting
```bash
# Format code with Black
black pyspark_tools tests

# Sort imports with isort
isort pyspark_tools tests
```

#### Linting
```bash
# Check with flake8
flake8 pyspark_tools tests --max-line-length=88 --extend-ignore=E203,W503
```

#### Testing
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=pyspark_tools --cov-report=term-missing

# Coverage must be >= 70%
pytest tests/ --cov=pyspark_tools --cov-fail-under=70
```

### 6. Pull Request Process

1. **Create PR with descriptive title**
   - Use conventional commit format
   - Example: `feat: add Redshift SUPER type support`

2. **Fill out PR description**
   - What changes were made?
   - Why were they made?
   - How to test?
   - Related issues?

3. **Ensure all CI checks pass**
   - ✅ Code quality (Black, isort, flake8)
   - ✅ Tests (all tests passing)
   - ✅ Coverage (>= 70%)
   - ✅ Build (package builds successfully)

4. **Request review**
   - At least 1 approval required for main branch
   - Address review comments

5. **Merge**
   - Squash and merge (preferred)
   - Rebase and merge (for clean history)
   - No merge commits to main

### 7. CI Pipeline

Our CI pipeline enforces quality gates:

```
┌─────────────┐
│  Validate   │  Quick syntax and structure checks
└──────┬──────┘
       │
┌──────▼──────┐
│Code Quality │  Black, isort, flake8, bandit
└──────┬──────┘
       │
┌──────▼──────┐
│    Tests    │  Unit tests on Python 3.11 & 3.12
└──────┬──────┘
       │
┌──────▼──────┐
│Integration  │  Integration and SQL conversion tests
└──────┬──────┘
       │
┌──────▼──────┐
│    Build    │  Package build and validation
└──────┬──────┘
       │
┌──────▼──────┐
│ Main Gate   │  Final checks for main branch
└─────────────┘
```

### 8. Testing Guidelines

#### Writing Tests
```python
# tests/test_my_feature.py
import pytest
from pyspark_tools.my_module import MyClass

class TestMyFeature:
    """Test suite for my feature."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.instance = MyClass()
    
    def test_basic_functionality(self):
        """Test basic functionality."""
        result = self.instance.do_something()
        assert result == expected_value
    
    def test_edge_case(self):
        """Test edge case handling."""
        with pytest.raises(ValueError):
            self.instance.do_something(invalid_input)
```

#### Test Coverage Requirements
- Minimum 70% overall coverage
- New code should have 80%+ coverage
- Critical paths must be fully tested

### 9. Documentation

- Update docstrings for new functions/classes
- Update README.md if adding major features
- Add examples for new functionality
- Update CHANGELOG.md

### 10. Branch Protection Rules

The `main` branch is protected with these rules:

- ✅ Require pull request before merging
- ✅ Require 1 approval
- ✅ Require status checks to pass
  - validate
  - code-quality
  - test (Python 3.11)
  - test (Python 3.12)
  - integration-tests
  - build
  - ci-success
- ✅ Require branches to be up to date
- ✅ Require conversation resolution
- ✅ No force pushes
- ✅ No deletions

### 11. Common Issues

#### Tests failing locally but passing in CI
```bash
# Ensure clean environment
deactivate
rm -rf .venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
pytest tests/
```

#### Import errors
```bash
# Reinstall in editable mode
pip install -e .
```

#### Coverage too low
```bash
# Check coverage report
pytest tests/ --cov=pyspark_tools --cov-report=html
open htmlcov/index.html
```

### 12. Getting Help

- 📖 Read the [documentation](docs/)
- 🐛 Report bugs via [GitHub Issues](https://github.com/your-org/pyspark_mcp/issues)
- 💬 Ask questions in [Discussions](https://github.com/your-org/pyspark_mcp/discussions)
- 📧 Contact maintainers

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on the code, not the person
- Help others learn and grow

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
