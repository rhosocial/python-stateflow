# tests/providers/mariadb_async.py
"""MariaDB async provider for stateflow tests (MariaDB 12.2)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mariadb import AsyncMariaDBBackend
from rhosocial.activerecord.backend.impl.mariadb.config import MariaDBConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import async_create_tables, async_drop_tables
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "192.168.1.3"
_PORT = 15691
_DB = "test_db"
_USER = "root"
_PWD = "password"


class MariaDBAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "mariadb-async"

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
        config = MariaDBConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD, charset="utf8mb4",
            autocommit=True, ssl_disabled=False,
        )
        group = AsyncBackendGroup(
            name="stateflow-mariadb-async", models=list(self.models),
            config=config, backend_class=AsyncMariaDBBackend,
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
