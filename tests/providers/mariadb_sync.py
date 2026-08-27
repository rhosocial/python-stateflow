# tests/providers/mariadb_sync.py
"""MariaDB sync provider for stateflow tests."""

import os
from typing import Sequence, Type

from rhosocial.activerecord.backend.impl.mariadb import MariaDBBackend
from rhosocial.activerecord.backend.impl.mariadb.config import MariaDBConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import create_tables, drop_tables
from rhosocial.stateflow.models import (
    FlowPath, Order, OrderEvent, OrderOutbox, OrderProcess,
    OrderSubProcess, OrderTemplate, OrderTemplateStep, SubProcessDependency,
)

from .base import StateflowSyncProvider


class MariaDBSyncProvider(StateflowSyncProvider):
    @property
    def name(self) -> str:
        return "mariadb-sync"

    @classmethod
    def is_available(cls) -> bool:
        import socket
        import os
        try:
            host = os.environ.get("STATEFLOW_MARIADB_HOST", "127.0.0.1")
            port = int(os.environ.get("STATEFLOW_MARIADB_PORT", "3306"))
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
        config = MariaDBConnectionConfig(
            host=os.environ.get("STATEFLOW_MARIADB_HOST", "127.0.0.1"),
            port=int(os.environ.get("STATEFLOW_MARIADB_PORT", "3306")),
            username=os.environ.get("STATEFLOW_MARIADB_USER", "root"),
            password=os.environ.get("STATEFLOW_MARIADB_PASSWORD", ""),
            database=os.environ.get("STATEFLOW_MARIADB_DB", "stateflow_test"),
        )
        group = BackendGroup(
            name="stateflow-mariadb-sync",
            models=list(self.models),
            config=config,
            backend_class=MariaDBBackend,
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
