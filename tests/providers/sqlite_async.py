# tests/providers/sqlite_async.py
"""SQLite async provider for stateflow tests.

Uses an in-memory SQLite database with a single shared async backend
instance (via ``AsyncBackendGroup``) so all async models participate in the
same async transaction scope.
"""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import async_create_tables, async_drop_tables
from rhosocial.stateflow.models import (
    AsyncFlowPath,
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderOutbox,
    AsyncOrderProcess,
    AsyncOrderSubProcess,
    AsyncOrderTemplate,
    AsyncOrderTemplateStep,
    AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider


class SQLiteAsyncProvider(StateflowAsyncProvider):
    """SQLite in-memory async provider."""

    @property
    def name(self) -> str:
        return "sqlite-async"

    @classmethod
    def is_available(cls) -> bool:
        return True

    @property
    def models(self) -> Sequence[Type]:
        return (
            AsyncOrderTemplate,
            AsyncOrderTemplateStep,
            AsyncFlowPath,
            AsyncOrder,
            AsyncOrderProcess,
            AsyncOrderSubProcess,
            AsyncSubProcessDependency,
            AsyncOrderEvent,
            AsyncOrderOutbox,
        )

    async def setup(self) -> object:
        config = SQLiteConnectionConfig(database=":memory:")
        group = AsyncBackendGroup(
            name="stateflow-sqlite-async",
            models=list(self.models),
            config=config,
            backend_class=AsyncSQLiteBackend,
        )
        await group.configure()
        backend = group.get_backend()
        await backend.connect()
        await backend.introspect_and_adapt()
        await async_create_tables(backend)
        return group

    async def teardown(self, handle: object) -> None:
        backend = handle.get_backend()
        await async_drop_tables(backend)
        await handle.disconnect()
