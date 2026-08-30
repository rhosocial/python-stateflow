# tests/providers/firebird_sync.py
"""Firebird sync provider for stateflow tests (Firebird 5)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.firebird import FirebirdBackend
from rhosocial.activerecord.backend.impl.firebird.config import FirebirdConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    FlowPath, Order, OrderEvent, OrderOutbox, OrderProcess,
    OrderSubProcess, OrderTemplate, OrderTemplateStep, SubProcessDependency,
)

from .base import StateflowSyncProvider

_HOST = "192.168.1.3"
_PORT = 19582
_DB = "/var/lib/firebird/data/test_db"
_USER = "SYSDBA"
_PWD = "password"


class FirebirdSyncProvider(StateflowSyncProvider):
    @property
    def name(self) -> str:
        return "firebird-sync"

    @property
    def models(self) -> Sequence[Type]:
        return (
            OrderTemplate, OrderTemplateStep, FlowPath,
            Order, OrderProcess, OrderSubProcess,
            SubProcessDependency, OrderEvent, OrderOutbox,
        )

    @classmethod
    def is_available(cls) -> bool:
        import socket
        try:
            with socket.create_connection((_HOST, _PORT), timeout=2):
                return True
        except (OSError, ConnectionError):
            return False

    def setup(self) -> object:
        config = FirebirdConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD, charset="UTF8",
        )
        group = BackendGroup(
            name="stateflow-firebird-sync", models=list(self.models),
            config=config, backend_class=FirebirdBackend,
        )
        group.configure()
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        Schema.create_tables(backend)
        return group

    def teardown(self, handle: object) -> None:
        backend = handle.get_backend()
        Schema.drop_tables(backend)
        handle.disconnect()
