# tests/providers/mysql_sync.py
"""MySQL sync provider for stateflow tests."""

import os
from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mysql import MySQLBackend
from rhosocial.activerecord.backend.impl.mysql.config import MySQLConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import create_tables, drop_tables
from rhosocial.stateflow.models import (
    FlowPath,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
)

from .base import StateflowSyncProvider


class MySQLSyncProvider(StateflowSyncProvider):
    """MySQL sync provider — uses a test database via env vars."""

    @property
    def name(self) -> str:
        return "mysql-sync"

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
            OrderTemplate, OrderTemplateStep, FlowPath,
            Order, OrderProcess, OrderSubProcess,
            SubProcessDependency, OrderEvent, OrderOutbox,
        )

    def setup(self) -> object:
        config = MySQLConnectionConfig(
            host=os.environ.get("STATEFLOW_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("STATEFLOW_MYSQL_PORT", "3306")),
            username=os.environ.get("STATEFLOW_MYSQL_USER", "root"),
            password=os.environ.get("STATEFLOW_MYSQL_PASSWORD", ""),
            database=os.environ.get("STATEFLOW_MYSQL_DB", "stateflow_test"),
        )
        group = BackendGroup(
            name="stateflow-mysql-sync",
            models=list(self.models),
            config=config,
            backend_class=MySQLBackend,
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
