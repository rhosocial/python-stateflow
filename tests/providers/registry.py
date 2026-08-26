# tests/providers/registry.py
"""Provider registry for stateflow multi-backend tests.

Importing this module registers all available providers. To add a new
backend (e.g. MySQL, PostgreSQL), create a provider module and import it
here — the rest of the test infrastructure picks it up automatically.
"""

from .base import StateflowAsyncProvider, StateflowSyncProvider
from .sqlite_async import SQLiteAsyncProvider
from .sqlite_sync import SQLiteSyncProvider


def get_sync_providers() -> list[StateflowSyncProvider]:
    """Return all registered sync providers."""
    return [SQLiteSyncProvider()]


def get_async_providers() -> list[StateflowAsyncProvider]:
    """Return all registered async providers."""
    return [SQLiteAsyncProvider()]


def get_sync_provider(name: str = "sqlite-sync") -> StateflowSyncProvider:
    """Return the sync provider matching ``name``."""
    for p in get_sync_providers():
        if p.name == name:
            return p
    raise ValueError(f"No sync provider named '{name}'")


def get_async_provider(name: str = "sqlite-async") -> StateflowAsyncProvider:
    """Return the async provider matching ``name``."""
    for p in get_async_providers():
        if p.name == name:
            return p
    raise ValueError(f"No async provider named '{name}'")
