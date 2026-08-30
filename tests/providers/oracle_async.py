# tests/providers/oracle_async.py
"""Oracle async provider for stateflow tests (Oracle 23c Free)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.oracle import AsyncOracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "127.0.0.1"
_PORT = 11523
_USER = "system"
_PWD = "Password1!"
_SERVICE = "FREEPDB1"


class OracleAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "oracle-async"

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
        config = OracleConnectionConfig(
            host=_HOST, port=_PORT, service_name=_SERVICE,
            username=_USER, password=_PWD, encoding="AL32UTF8",
        )
        group = AsyncBackendGroup(
            name="stateflow-oracle-async", models=list(self.models),
            config=config, backend_class=AsyncOracleBackend,
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
