# tests/providers/registry.py
"""Provider registry for stateflow multi-backend tests.

Discovers available backends at import time and registers sync + async
providers for each. Backends that are not installed are silently skipped;
tests parameterized by provider only run against installed backends.

Currently supported: SQLite, MySQL, MariaDB, PostgreSQL, SQL Server, Oracle,
Firebird. To add a new backend, implement a provider module and import it
here — the rest of the test infrastructure picks it up automatically.
"""

from typing import List

from .base import StateflowAsyncProvider, StateflowSyncProvider


def _try_import(module_path: str):
    try:
        __import__(module_path)
        return True
    except ImportError:
        return False


def get_sync_providers() -> List[StateflowSyncProvider]:
    """Return all registered sync providers for available backends.

    A backend is included only if:
    1. The backend Python module is importable, AND
    2. ``is_available()`` returns True (server reachable).
    """
    providers: List[StateflowSyncProvider] = []

    from .sqlite_sync import SQLiteSyncProvider
    if SQLiteSyncProvider.is_available():
        providers.append(SQLiteSyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.mysql"):
        from .mysql_sync import MySQLSyncProvider
        if MySQLSyncProvider.is_available():
            providers.append(MySQLSyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.mariadb"):
        from .mariadb_sync import MariaDBSyncProvider
        if MariaDBSyncProvider.is_available():
            providers.append(MariaDBSyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.postgres"):
        from .postgres_sync import PostgresSyncProvider
        if PostgresSyncProvider.is_available():
            providers.append(PostgresSyncProvider())

    return providers


def get_async_providers() -> List[StateflowAsyncProvider]:
    """Return all registered async providers for available backends."""
    providers: List[StateflowAsyncProvider] = []

    from .sqlite_async import SQLiteAsyncProvider
    if SQLiteAsyncProvider.is_available():
        providers.append(SQLiteAsyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.mysql"):
        from .mysql_async import MySQLAsyncProvider
        if MySQLAsyncProvider.is_available():
            providers.append(MySQLAsyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.mariadb"):
        from .mariadb_async import MariaDBAsyncProvider
        if MariaDBAsyncProvider.is_available():
            providers.append(MariaDBAsyncProvider())

    if _try_import("rhosocial.activerecord.backend.impl.postgres"):
        from .postgres_async import PostgresAsyncProvider
        if PostgresAsyncProvider.is_available():
            providers.append(PostgresAsyncProvider())

    return providers


def get_sync_provider(name: str) -> StateflowSyncProvider:
    for p in get_sync_providers():
        if p.name == name:
            return p
    raise ValueError(f"No sync provider named '{name}'")


def get_async_provider(name: str) -> StateflowAsyncProvider:
    for p in get_async_providers():
        if p.name == name:
            return p
    raise ValueError(f"No async provider named '{name}'")
