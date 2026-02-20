# Git Hooks

This directory contains git hooks to maintain code quality.

## Installation

Run the installation script to set up the hooks:

```bash
bash .githooks/install.sh
```

Or manually:

```bash
chmod +x .githooks/pre-push
ln -sf ../../.githooks/pre-push .git/hooks/pre-push
```

## Available Hooks

### pre-push

Runs before pushing code to remote. This hook:
- Formats Python code with `black`
- Sorts imports with `isort`
- Automatically stages formatted files

**Requirements:**
```bash
pip install black isort
```

**Bypass hook (not recommended):**
```bash
git push --no-verify
```

## Uninstall

To remove hooks:

```bash
rm .git/hooks/pre-push
```
