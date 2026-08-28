# tests/providers/postgres_async.py
"""PostgreSQL async provider for stateflow tests (PostgreSQL 19 beta3)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "192.168.1.3"
_PORT = 16690
_DB = "test_db"
_USER = "root"
_PWD = "password"


class PostgresAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "postgres-async"

    @property
    def models(self) -> Sequence[Type]:
        return (
            AsyncOrderTemplate, AsyncOrderTemplateStep, AsyncFlowPath,
            AsyncOrder, AsyncOrderProcess, AsyncOrderSubProcess,
            AsyncSubProcessDependency, AsyncOrderEvent, AsyncOrderOutbox,
        )

    @classmethod
    def is_available(cls) -> bool:
        import socket
        try:
            with socket.create_connection((_HOST, _PORT), timeout=2):
                return True
        except (OSError, ConnectionError):
            return False

    async def setup(self) -> object:
        config = PostgresConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD,
        )
        group = AsyncBackendGroup(
            name="stateflow-postgres-async", models=list(self.models),
            config=config, backend_class=AsyncPostgresBackend,
        )
        await group.configure()
        backend = group.get_backend()
        await backend.connect()
        await backend.introspect_and_adapt()
        await Schema.async_create_tables(backend)
        return group

    async def teardown(self, handle: object) -> None:
        backend = handle.get_backend()
        await Schema.async_drop_tables(backend)
        await handle.disconnect()
