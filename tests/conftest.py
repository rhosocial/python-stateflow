# tests/conftest.py
"""Pytest configuration for stateflow tests.

Provides backend-parameterized fixtures via the provider registry in
``tests/providers/``. To add a new backend, implement a provider and
register it in ``providers/registry.py`` — no test changes required.
"""

import os
import sys

# Make the tests/ directory importable so `from providers.registry import ...`
# works without requiring PYTHONPATH=tests at runtime. This is the same
# pattern used by rhosocial-activerecord (via PYTHONPATH=tests) but kept
# self-contained here so plain `pytest` works out of the box.
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from providers.registry import get_async_provider, get_sync_provider


# ---------------------------------------------------------------------------
# Sync fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sync_provider():
    """Return the sync backend provider (default: sqlite-sync)."""
    return get_sync_provider()


@pytest.fixture
def backend_group(sync_provider):
    """Create and tear down a sync backend with all stateflow tables."""
    handle = sync_provider.setup()
    yield handle
    sync_provider.teardown(handle)


# ---------------------------------------------------------------------------
# Async fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def async_provider():
    """Return the async backend provider (default: sqlite-async)."""
    return get_async_provider()


@pytest.fixture
async def async_backend_group(async_provider):
    """Create and tear down an async backend with all stateflow tables."""
    handle = await async_provider.setup()
    yield handle
    await async_provider.teardown(handle)
