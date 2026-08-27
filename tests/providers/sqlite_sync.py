# tests/providers/sqlite_sync.py
"""SQLite sync provider for stateflow tests.

Uses an in-memory SQLite database with a single shared backend instance
(via ``BackendGroup``) so all sync models participate in the same
transaction scope.
"""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import create_tables, drop_tables
from rhosocial.stateflow.models import (
    FlowPath,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
)

from .base import StateflowSyncProvider


class SQLiteSyncProvider(StateflowSyncProvider):
    """SQLite in-memory sync provider."""

    @property
    def name(self) -> str:
        return "sqlite-sync"

    @property
    def models(self) -> Sequence[Type]:
        return (
            OrderTemplate,
            OrderTemplateStep,
            FlowPath,
            Order,
            OrderProcess,
            OrderSubProcess,
            SubProcessDependency,
            OrderEvent,
            OrderOutbox,
        )

    def setup(self) -> object:
        config = SQLiteConnectionConfig(database=":memory:")
        group = BackendGroup(
            name="stateflow-sqlite-sync",
            models=list(self.models),
            config=config,
            backend_class=SQLiteBackend,
        )
        group.configure()
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        return group

    def teardown(self, handle: object) -> None:
        backend = handle.get_backend()
        drop_tables(backend)
        handle.disconnect()
