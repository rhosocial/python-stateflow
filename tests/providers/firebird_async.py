# tests/providers/firebird_async.py
"""Firebird async provider for stateflow tests (Firebird 5)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.firebird import AsyncFirebirdBackend
from rhosocial.activerecord.backend.impl.firebird.config import FirebirdConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    AsyncFlowPath, AsyncOrder, AsyncOrderEvent, AsyncOrderOutbox,
    AsyncOrderProcess, AsyncOrderSubProcess, AsyncOrderTemplate,
    AsyncOrderTemplateStep, AsyncSubProcessDependency,
)

from .base import StateflowAsyncProvider

_HOST = "192.168.1.3"
_PORT = 19582
_DB = "/var/lib/firebird/data/test_db"
_USER = "SYSDBA"
_PWD = "password"


class FirebirdAsyncProvider(StateflowAsyncProvider):
    @property
    def name(self) -> str:
        return "firebird-async"

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
        config = FirebirdConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD, charset="UTF8",
        )
        group = AsyncBackendGroup(
            name="stateflow-firebird-async", models=list(self.models),
            config=config, backend_class=AsyncFirebirdBackend,
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
