# tests/conftest.py
"""Pytest configuration for stateflow tests.

Discovers all available backend providers (SQLite, MySQL, MariaDB, etc.)
and parameterizes fixtures so tests run against every installed backend.

To add a new backend, implement a provider module and register it in
``providers/registry.py`` — no test changes required.
"""

import os
import sys

# Make the tests/ directory importable for `from providers.registry import ...`.
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from providers.registry import get_async_providers, get_sync_providers


# ---------------------------------------------------------------------------
# Provider discovery
# ---------------------------------------------------------------------------

_SYNC_PROVIDERS = get_sync_providers()
_ASYNC_PROVIDERS = get_async_providers()

_SYNC_PARAMS = [pytest.param(p, id=p.name) for p in _SYNC_PROVIDERS]
_ASYNC_PARAMS = [pytest.param(p, id=p.name) for p in _ASYNC_PROVIDERS]


# ---------------------------------------------------------------------------
# Sync fixtures (parameterized across all available sync providers)
# ---------------------------------------------------------------------------

@pytest.fixture(params=_SYNC_PARAMS)
def sync_provider(request):
    """Return the current sync backend provider."""
    return request.param


@pytest.fixture
def backend_group(sync_provider):
    """Create and tear down a sync backend with all stateflow tables."""
    handle = sync_provider.setup()
    yield handle
    sync_provider.teardown(handle)


# ---------------------------------------------------------------------------
# Async fixtures (Parameterized across all available async providers)
# ---------------------------------------------------------------------------

@pytest.fixture(params=_ASYNC_PARAMS)
async def async_provider(request):
    """Return the current async backend provider."""
    return request.param


@pytest.fixture
async def async_backend_group(async_provider):
    """Create and tear down an async backend with all stateflow tables."""
    handle = await async_provider.setup()
    yield handle
    await async_provider.teardown(handle)
