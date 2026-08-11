"""
Shared pytest fixtures for local (offline) unit tests.

STATUS: written 2026-08-11, NEVER RUN. This project's declared test dependency is
databricks-connect (pyproject.toml), which requires a live cluster or serverless
endpoint to create a SparkSession — the same Databricks compute currently blocked
(see AGENTS.md section 8). These fixtures assume a *local*, open-source `pyspark`
instead (SparkSession.builder.master("local[*]")), which cannot coexist with
databricks-connect in the same environment (both ship a `pyspark` package).

To actually run these tests:
  1. Create a separate venv that does NOT include databricks-connect.
  2. `pip install pyspark pytest` in that venv (plain PyPI pyspark, not databricks-connect).
  3. Run `pytest` from that venv.

See AGENTS.md section 8 for the full pending-setup note.
"""

import sys
import types
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src"


def _install_pipelines_stub():
    """`from pyspark import pipelines as dp` only works inside the real Lakeflow
    runtime — `pipelines` is injected there, it doesn't exist in open-source pyspark.
    Install a fake module so pipeline source files can be imported locally: every
    `@dp.xxx(...)` decorator becomes a no-op that returns the original function
    unchanged, so the *real* function bodies (including helpers like
    _apply_min_group_size) are exactly what gets tested."""

    def _identity_decorator_factory(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

    stub = types.ModuleType("pyspark.pipelines")
    for name in [
        "table",
        "materialized_view",
        "temporary_view",
        "expect",
        "expect_or_drop",
        "expect_or_fail",
        "expect_all",
        "expect_all_or_drop",
        "expect_all_or_fail",
        "append_flow",
    ]:
        setattr(stub, name, _identity_decorator_factory)
    sys.modules["pyspark.pipelines"] = stub


def import_pipeline_module(relative_path, spark):
    """Import a src/**/*.py pipeline file as a plain module, with the `dp` stub
    installed and the `spark` global injected (pipeline files reference a bare
    `spark` name that the Lakeflow runtime normally provides). Returns the
    imported module so tests can reach its real functions directly."""
    import importlib.util

    _install_pipelines_stub()

    module_path = SRC_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(relative_path.replace("/", "_"), module_path)
    module = importlib.util.module_from_spec(spec)
    module.spark = spark  # inject the global the Lakeflow runtime would normally provide
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark():
    """Local Spark session — requires plain `pyspark` installed (NOT databricks-connect,
    see module docstring). Not verified to work in this repo's current environment."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("hubble_learning_bi-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
