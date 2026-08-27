# tests/providers/mysql_async.py
"""MySQL async provider for stateflow tests."""

import os
from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mysql import AsyncMySQLBackend
from rhosocial.activerecord.backend.impl.mysql.config import MySQLConnectionConfig
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


class MySQLAsyncProvider(StateflowAsyncProvider):
    """MySQL async provider — uses a test database via env vars."""

    @property
    def name(self) -> str:
        return "mysql-async"

    @classmethod
    def is_available(cls) -> bool:
        import socket
        import os
        try:
            host = os.environ.get("STATEFLOW_MYSQL_HOST", "127.0.0.1")
            port = int(os.environ.get("STATEFLOW_MYSQL_PORT", "3306"))
            with socket.create_connection((host, port), timeout=1):
                return True
        except (OSError, ConnectionError):
            return False

    @property
    def models(self) -> Sequence[Type]:
        return (
            AsyncOrderTemplate, AsyncOrderTemplateStep, AsyncFlowPath,
            AsyncOrder, AsyncOrderProcess, AsyncOrderSubProcess,
            AsyncSubProcessDependency, AsyncOrderEvent, AsyncOrderOutbox,
        )

    async def setup(self) -> object:
        config = MySQLConnectionConfig(
            host=os.environ.get("STATEFLOW_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("STATEFLOW_MYSQL_PORT", "3306")),
            username=os.environ.get("STATEFLOW_MYSQL_USER", "root"),
            password=os.environ.get("STATEFLOW_MYSQL_PASSWORD", ""),
            database=os.environ.get("STATEFLOW_MYSQL_DB", "stateflow_test"),
        )
        group = AsyncBackendGroup(
            name="stateflow-mysql-async",
            models=list(self.models),
            config=config,
            backend_class=AsyncMySQLBackend,
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
