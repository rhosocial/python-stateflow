# tests/providers/oracle_sync.py
"""Oracle sync provider for stateflow tests (Oracle 23c Free)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.oracle import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    FlowPath, Order, OrderEvent, OrderOutbox, OrderProcess,
    OrderSubProcess, OrderTemplate, OrderTemplateStep, SubProcessDependency,
)

from .base import StateflowSyncProvider

_HOST = "127.0.0.1"
_PORT = 11523
_USER = "system"
_PWD = "Password1!"
_SERVICE = "FREEPDB1"


class OracleSyncProvider(StateflowSyncProvider):
    @property
    def name(self) -> str:
        return "oracle-sync"

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
        config = OracleConnectionConfig(
            host=_HOST, port=_PORT, service_name=_SERVICE,
            username=_USER, password=_PWD, encoding="AL32UTF8",
        )
        group = BackendGroup(
            name="stateflow-oracle-sync", models=list(self.models),
            config=config, backend_class=OracleBackend,
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
