# tests/providers/mysql_sync.py
"""MySQL sync provider for stateflow tests (MySQL 9.7)."""

from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mysql import MySQLBackend
from rhosocial.activerecord.backend.impl.mysql.config import MySQLConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import create_tables, drop_tables
from rhosocial.stateflow.models import (
    FlowPath, Order, OrderEvent, OrderOutbox, OrderProcess,
    OrderSubProcess, OrderTemplate, OrderTemplateStep, SubProcessDependency,
)

from .base import StateflowSyncProvider

_HOST = "192.168.1.3"
_PORT = 14686
_DB = "test_db"
_USER = "root"
_PWD = "password"


class MySQLSyncProvider(StateflowSyncProvider):
    @property
    def name(self) -> str:
        return "mysql-sync"

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
        config = MySQLConnectionConfig(
            host=_HOST, port=_PORT, database=_DB,
            username=_USER, password=_PWD, charset="utf8mb4",
            autocommit=True, ssl_disabled=False,
        )
        group = BackendGroup(
            name="stateflow-mysql-sync", models=list(self.models),
            config=config, backend_class=MySQLBackend,
        )
        group.configure()
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        return group

    def teardown(self, handle: object) -> None:
        backend = handle.get_backend()
        drop_tables(backend)
        handle.disconnect()
