# tests/providers/sqlserver_sync.py
"""SQL Server sync provider for stateflow tests (SQL Server 2025)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.sqlserver import SQLServerBackend
from rhosocial.activerecord.backend.impl.sqlserver.config import SQLServerConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import Schema
from rhosocial.stateflow.models import (
    FlowPath, Order, OrderEvent, OrderOutbox, OrderProcess,
    OrderSubProcess, OrderTemplate, OrderTemplateStep, SubProcessDependency,
)

from .base import StateflowSyncProvider

_HOST = "127.0.0.1"
_PORT = 11435
_DB = "master"
_USER = "sa"
_PWD = "Password123!"


class SQLServerSyncProvider(StateflowSyncProvider):
    @property
    def name(self) -> str:
        return "sqlserver-sync"

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
        config = SQLServerConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD,
            driver="ODBC Driver 17 for SQL Server",
            encrypt=False, trust_server_certificate=True,
            autocommit=True,
        )
        group = BackendGroup(
            name="stateflow-sqlserver-sync", models=list(self.models),
            config=config, backend_class=SQLServerBackend,
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
