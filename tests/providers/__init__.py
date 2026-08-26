# tests/providers/__init__.py
"""Stateflow test provider registry.

Each backend (SQLite, MySQL, PostgreSQL, …) provides a sync and/or async
provider that knows how to:

1. Configure all stateflow models on that backend.
2. Create the stateflow schema (DDL).
3. Tear down the schema after tests.

Tests use the provider via fixtures in ``conftest.py``; they never touch
backend-specific code directly. To add a new backend, implement
:class:`StateflowSyncProvider` or :class:`StateflowAsyncProvider` and
register it in ``registry.py``.
"""
