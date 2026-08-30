# tests/providers/sqlserver_async.py
"""SQL Server async provider for stateflow tests (SQL Server 2025)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.sqlserver import AsyncSQLServerBackend
from rhosocial.activerecord.backend.impl.sqlserver.config import SQLServerConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "127.0.0.1"
_PORT = 11435
_DB = "master"
_USER = "sa"
_PWD = "Password123!"


class SQLServerAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "sqlserver-async"

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
        config = SQLServerConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD,
            driver="ODBC Driver 17 for SQL Server",
            encrypt=False, trust_server_certificate=True,
            autocommit=True,
        )
        group = AsyncBackendGroup(
            name="stateflow-sqlserver-async", models=list(self.models),
            config=config, backend_class=AsyncSQLServerBackend,
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
