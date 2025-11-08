# Contributing to PySpark MCP Server

Thank you for your interest in contributing to the PySpark MCP Server! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- Git
- Basic knowledge of PySpark and SQL

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/yourusername/pyspark-mcp-server.git
   cd pyspark-mcp-server
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Run Tests**
   ```bash
   pytest
   ```

## 🛠️ Development Guidelines

### Code Style
- Use [Black](https://black.readthedocs.io/) for code formatting
- Follow PEP 8 guidelines
- Use type hints where appropriate
- Write docstrings for all public functions and classes

### Testing
- Write tests for new features
- Ensure all tests pass before submitting PR
- Aim for good test coverage
- Use pytest for testing framework

### Commit Messages
Follow conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/changes
- `refactor:` for code refactoring

Example: `feat: add support for window functions in SQL conversion`

## 🔄 Pull Request Process

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**
   - Write code following the guidelines above
   - Add tests for new functionality
   - Update documentation if needed

3. **Test Your Changes**
   ```bash
   pytest
   black pyspark_tools/
   mypy pyspark_tools/
   ```

4. **Commit and Push**
   ```bash
   git add .
   git commit -m "feat: your descriptive commit message"
   git push origin feature/your-feature-name
   ```

5. **Create Pull Request**
   - Use the GitHub interface to create a PR
   - Fill out the PR template
   - Link any related issues

## 🐛 Bug Reports

When reporting bugs, please include:
- Python version
- PySpark version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages/logs

## 💡 Feature Requests

For feature requests, please:
- Check if the feature already exists
- Describe the use case
- Explain why it would be valuable
- Consider implementation complexity

## 📝 Documentation

- Update README.md for user-facing changes
- Add docstrings for new functions/classes
- Update examples if behavior changes
- Consider adding usage examples

## 🤝 Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow the golden rule

## 📞 Getting Help

- Open an issue for questions
- Check existing issues and discussions
- Review the documentation

Thank you for contributing! 🎉