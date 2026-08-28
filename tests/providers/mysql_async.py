# tests/providers/mysql_async.py
"""MySQL async provider for stateflow tests (MySQL 9.7)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mysql import AsyncMySQLBackend
from rhosocial.activerecord.backend.impl.mysql.config import MySQLConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "192.168.1.3"
_PORT = 14686
_DB = "test_db"
_USER = "root"
_PWD = "password"


class MySQLAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "mysql-async"

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
        config = MySQLConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD, charset="utf8mb4",
            autocommit=True, ssl_disabled=False,
        )
        group = AsyncBackendGroup(
            name="stateflow-mysql-async", models=list(self.models),
            config=config, backend_class=AsyncMySQLBackend,
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
