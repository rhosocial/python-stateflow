# src/rhosocial/stateflow/schema.py
"""Schema helpers for stateflow tables.

The DDL strings here are SQLite-flavored by default (stateflow ships with the
SQLite backend out of the box). Backend-specific DDL is intentionally left to
the multi-backend testsuite integration layer; see ``changelog.d`` for the
roadmap. Every table is created with ``IF NOT EXISTS`` so the helper is
idempotent and safe to call from fixtures and application bootstrap.
"""


from .models import (
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

SQLITE_DDL: str = """
CREATE TABLE IF NOT EXISTS stateflow_order_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'draft',
    description TEXT,
    published_at TEXT,
    deprecated_at TEXT,
    created_by TEXT,
    checksum TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_order_template_steps (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    name TEXT NOT NULL,
    handler_class TEXT NOT NULL,
    terminal_states TEXT NOT NULL DEFAULT '[]',
    advance_states TEXT NOT NULL DEFAULT '[]',
    rollback_states TEXT NOT NULL DEFAULT '[]',
    timeout_seconds INTEGER,
    timeout_status TEXT,
    on_start_notify TEXT,
    on_complete_notify TEXT,
    on_rollback_notify TEXT,
    on_timeout_notify TEXT,
    depends_on TEXT NOT NULL DEFAULT '[]',
    step_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_flow_paths (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    name TEXT NOT NULL,
    skip_steps TEXT NOT NULL DEFAULT '[]',
    start_from TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_orders (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    context TEXT NOT NULL DEFAULT '{}',
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_order_processes (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    template_snapshot TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_order_subprocesses (
    id TEXT PRIMARY KEY,
    process_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    handler_class TEXT NOT NULL,
    terminal_states TEXT NOT NULL DEFAULT '[]',
    advance_states TEXT NOT NULL DEFAULT '[]',
    rollback_states TEXT NOT NULL DEFAULT '[]',
    timeout_seconds INTEGER,
    timeout_status TEXT,
    started_at TEXT,
    timeout_at TEXT,
    completed_at TEXT,
    skipped INTEGER NOT NULL DEFAULT 0,
    extra TEXT NOT NULL DEFAULT '{}',
    source TEXT NOT NULL DEFAULT 'template',
    sequence INTEGER NOT NULL DEFAULT 0,
    created_event_id TEXT,
    is_reversible INTEGER NOT NULL DEFAULT 0,
    rollback_status TEXT NOT NULL DEFAULT 'not_required',
    rollback_started_at TEXT,
    rollback_completed_at TEXT,
    rollback_error TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_subprocess_dependencies (
    id TEXT PRIMARY KEY,
    process_id TEXT NOT NULL,
    subprocess_id TEXT NOT NULL,
    depends_on_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_order_events (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    subprocess_id TEXT,
    event_type TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    payload TEXT NOT NULL DEFAULT '{}',
    event_key TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    conflict INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stateflow_order_outbox (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

ALL_MODELS: tuple = (
    OrderTemplate,
    OrderTemplateStep,
    FlowPath,
    Order,
    OrderProcess,
    OrderSubProcess,
    SubProcessDependency,
    OrderEvent,
    OrderOutbox,
)


def create_tables(backend, *, ddl: str = SQLITE_DDL) -> None:
    """Create all stateflow tables on the given backend.

    Executes the DDL statements inside the backend's default execution
    context. The caller is responsible for committing the transaction if the
    backend does not auto-commit DDL (SQLite does; some backends do not).

    Args:
        backend: A configured ``StorageBackend`` instance.
        ddl: Optional override DDL string; defaults to ``SQLITE_DDL``.
    """
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for statement in (stmt for stmt in ddl.split(";") if stmt.strip()):
        backend.execute(statement, options=options)


def drop_tables(backend) -> None:
    """Drop all stateflow tables. Useful for test teardown."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    drop_ddl = "\n".join(
        f"DROP TABLE IF EXISTS {model.__table_name__};" for model in reversed(ALL_MODELS)
    )
    for statement in (stmt for stmt in drop_ddl.split(";") if stmt.strip()):
        backend.execute(statement, options=options)


async def async_create_tables(backend, *, ddl: str = SQLITE_DDL) -> None:
    """Create all stateflow tables on the given async backend.

    Async counterpart of :func:`create_tables` — uses ``await
    backend.execute()`` for every DDL statement.
    """
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for statement in (stmt for stmt in ddl.split(";") if stmt.strip()):
        await backend.execute(statement, options=options)


async def async_drop_tables(backend) -> None:
    """Drop all stateflow tables on the given async backend."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    drop_ddl = "\n".join(
        f"DROP TABLE IF EXISTS {model.__table_name__};" for model in reversed(ALL_MODELS)
    )
    for statement in (stmt for stmt in drop_ddl.split(";") if stmt.strip()):
        await backend.execute(statement, options=options)


__all__ = [
    "ALL_MODELS",
    "SQLITE_DDL",
    "create_tables",
    "drop_tables",
]
