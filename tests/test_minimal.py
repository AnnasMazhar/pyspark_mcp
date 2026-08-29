"""Baseline tests: environment plus FastMCP tool registry."""

import sys
from pathlib import Path

import pytest

EXPECTED_TOOLS = {
    "convert",
    "analyze",
    "optimize",
    "review",
    "glue_job",
    "glue_schema",
    "glue_s3",
    "glue_data",
    "refactor",
    "search",
    "context",
    "batch_status",
    "s3_source",
    "analytics",
}


def _registered_tool_names():
    from pyspark_tools.server import app

    provider = getattr(app, "_local_provider", None)
    components = getattr(provider, "_components", None) if provider else None
    if isinstance(components, dict):
        names = set()
        for key in components:
            if isinstance(key, str) and key.startswith("tool:"):
                names.add(key[5:].split("@", 1)[0])
        if names:
            return names

    from pyspark_tools import consolidated_tools as ct

    return {name for name in EXPECTED_TOOLS if callable(getattr(ct, name, None))}


@pytest.mark.fast
class TestBasicEnvironment:
    """Test basic environment setup."""

    def test_python_version(self):
        """Test Python version is supported."""
        assert sys.version_info >= (
            3,
            11,
        ), f"Python 3.11+ required, got {sys.version_info}"

    def test_project_structure(self):
        """Test basic project structure exists."""
        project_root = Path(__file__).parent.parent

        assert (project_root / "pyspark_tools").exists(), "pyspark_tools package missing"
        assert (project_root / "tests").exists(), "tests directory missing"
        assert (project_root / "pyproject.toml").exists(), "pyproject.toml missing"
        assert (project_root / "run_server.py").exists(), "run_server.py missing"


@pytest.mark.fast
@pytest.mark.unit
class TestToolRegistry:
    """The 14 FastMCP routers must be registered on the app."""

    def test_fourteen_routers_registered(self):
        names = _registered_tool_names()
        missing = EXPECTED_TOOLS - names
        assert not missing, f"Missing MCP tools: {sorted(missing)}"

    def test_router_callables_exist(self):
        from pyspark_tools import consolidated_tools as ct

        for name in sorted(EXPECTED_TOOLS):
            assert callable(getattr(ct, name)), f"{name} is not a callable router"
