# src/rhosocial/stateflow/schema.py
"""Schema helpers for stateflow tables using ActiveRecord DDL expressions.

Table definitions use :class:`~rhosocial.activerecord.backend.expression.CreateTableExpression`
so the same definition compiles to correct DDL for every supported backend
(SQLite, MySQL, PostgreSQL, MariaDB, etc.) via the backend's dialect.

Column types are deliberately portable:
- UUID / datetime / JSON → ``TextType`` (stored as string, adapter handles conversion)
- int → ``IntegerType``
- bool → ``IntegerType`` (0/1, works on all backends)

This makes stateflow a real-world cross-backend test case for
rhosocial-activerecord's dialect system.

Note: expression imports are deferred to function call time to avoid
the ~10s import cost of the expression module at package load.
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


# ---------------------------------------------------------------------------
# Column spec helpers (expression imports are deferred)
# ---------------------------------------------------------------------------

def _col(name, type_instance, *, pk=False, not_null=False, default=None):
    """Build a ColumnDefinition from a compact spec."""
    from rhosocial.activerecord.backend.expression import (
        ColumnConstraint,
        ColumnConstraintType,
        ColumnDefinition,
    )
    constraints = []
    if pk:
        constraints.append(ColumnConstraint(ColumnConstraintType.PRIMARY_KEY))
    if not_null:
        constraints.append(ColumnConstraint(ColumnConstraintType.NOT_NULL))
    if default is not None:
        constraints.append(ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=default))
    return ColumnDefinition(name, type_instance, constraints=constraints)


def _table(dialect, name, columns):
    """Build a CREATE TABLE IF NOT EXISTS expression."""
    from rhosocial.activerecord.backend.expression import CreateTableExpression
    return CreateTableExpression(
        dialect=dialect, table=name, if_not_exists=True, columns=columns,
    )


# ---------------------------------------------------------------------------
# Table column definitions (shared across all backends)
# ---------------------------------------------------------------------------

def _table_expressions(dialect):
    """Return all 9 CreateTableExpression objects compiled for ``dialect``."""
    from rhosocial.activerecord.backend.expression.types import BooleanType, IntegerType, TextType, VarCharType

    _T = TextType()
    _I = IntegerType()
    _B = BooleanType()
    _ID = VarCharType(36)  # UUID v4 string is exactly 36 chars

    return [
        # 1. stateflow_order_templates
        _table(dialect, "stateflow_order_templates", [
            _col("id", _ID, pk=True),
            _col("name", _T, not_null=True),
            _col("version", _I, not_null=True),
            _col("status", _T, not_null=True),
            _col("description", _T),
            _col("published_at", _T),
            _col("deprecated_at", _T),
            _col("created_by", _T),
            _col("checksum", _T),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 2. stateflow_order_template_steps
        _table(dialect, "stateflow_order_template_steps", [
            _col("id", _ID, pk=True),
            _col("template_id", _T, not_null=True),
            _col("name", _T, not_null=True),
            _col("handler_class", _T, not_null=True),
            _col("terminal_states", _T, not_null=True),
            _col("advance_states", _T, not_null=True),
            _col("rollback_states", _T, not_null=True),
            _col("timeout_seconds", _I),
            _col("timeout_status", _T),
            _col("on_start_notify", _T),
            _col("on_complete_notify", _T),
            _col("on_rollback_notify", _T),
            _col("on_timeout_notify", _T),
            _col("depends_on", _T, not_null=True),
            _col("step_order", _I, not_null=True),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 3. stateflow_flow_paths
        _table(dialect, "stateflow_flow_paths", [
            _col("id", _ID, pk=True),
            _col("template_id", _T, not_null=True),
            _col("name", _T, not_null=True),
            _col("skip_steps", _T, not_null=True),
            _col("start_from", _T),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 4. stateflow_orders
        _table(dialect, "stateflow_orders", [
            _col("id", _ID, pk=True),
            _col("template_id", _T, not_null=True),
            _col("status", _T, not_null=True),
            _col("context", _T, not_null=True),
            _col("started_at", _T),
            _col("completed_at", _T),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 5. stateflow_order_processes
        _table(dialect, "stateflow_order_processes", [
            _col("id", _ID, pk=True),
            _col("order_id", _T, not_null=True),
            _col("template_snapshot", _T, not_null=True),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 6. stateflow_order_subprocesses
        _table(dialect, "stateflow_order_subprocesses", [
            _col("id", _ID, pk=True),
            _col("process_id", _T, not_null=True),
            _col("step_name", _T, not_null=True),
            _col("status", _T, not_null=True),
            _col("handler_class", _T, not_null=True),
            _col("terminal_states", _T, not_null=True),
            _col("advance_states", _T, not_null=True),
            _col("rollback_states", _T, not_null=True),
            _col("timeout_seconds", _I),
            _col("timeout_status", _T),
            _col("started_at", _T),
            _col("timeout_at", _T),
            _col("completed_at", _T),
            _col("skipped", _B, not_null=True),
            _col("extra", _T, not_null=True),
            _col("source", _T, not_null=True),
            _col("sequence", _I, not_null=True),
            _col("created_event_id", _T),
            _col("is_reversible", _B, not_null=True),
            _col("rollback_status", _T, not_null=True),
            _col("rollback_started_at", _T),
            _col("rollback_completed_at", _T),
            _col("rollback_error", _T),
            _col("version", _I, not_null=True),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 7. stateflow_subprocess_dependencies
        _table(dialect, "stateflow_subprocess_dependencies", [
            _col("id", _ID, pk=True),
            _col("process_id", _T, not_null=True),
            _col("subprocess_id", _T, not_null=True),
            _col("depends_on_id", _T, not_null=True),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 8. stateflow_order_events
        _table(dialect, "stateflow_order_events", [
            _col("id", _ID, pk=True),
            _col("order_id", _T, not_null=True),
            _col("subprocess_id", _T),
            _col("event_type", _T, not_null=True),
            _col("from_status", _T),
            _col("to_status", _T),
            _col("payload", _T, not_null=True),
            _col("event_key", _T),
            _col("correlation_id", _T),
            _col("causation_id", _T),
            _col("conflict", _B, not_null=True),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
        # 9. stateflow_order_outbox
        _table(dialect, "stateflow_order_outbox", [
            _col("id", _ID, pk=True),
            _col("event_id", _T, not_null=True),
            _col("topic", _T, not_null=True),
            _col("payload", _T, not_null=True),
            _col("status", _T, not_null=True),
            _col("retry_count", _I, not_null=True),
            _col("next_retry_at", _T),
            _col("created_at", _T, not_null=True),
            _col("updated_at", _T, not_null=True),
        ]),
    ]


def create_tables(backend, *, ddl=None) -> None:
    """Create all stateflow tables on the given backend.

    Uses the backend's dialect to compile ``CreateTableExpression`` objects
    into backend-specific DDL. Works for any supported backend (SQLite, MySQL,
    PostgreSQL, MariaDB, etc.) without per-backend SQL files.
    """
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    dialect = backend.dialect
    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for expr in _table_expressions(dialect):
        sql, _ = expr.to_sql()
        backend.execute(sql, options=options)


def drop_tables(backend) -> None:
    """Drop all stateflow tables."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for model in reversed(ALL_MODELS):
        backend.execute(
            f"DROP TABLE IF EXISTS {model.__table_name__}",
            options=options,
        )


async def async_create_tables(backend, *, ddl=None) -> None:
    """Create all stateflow tables on the given async backend."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    dialect = backend.dialect
    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for expr in _table_expressions(dialect):
        sql, _ = expr.to_sql()
        await backend.execute(sql, options=options)


async def async_drop_tables(backend) -> None:
    """Drop all stateflow tables on the given async backend."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    options = ExecutionOptions(stmt_type=StatementType.DDL)
    for model in reversed(ALL_MODELS):
        await backend.execute(
            f"DROP TABLE IF EXISTS {model.__table_name__}",
            options=options,
        )


__all__ = [
    "ALL_MODELS",
    "async_create_tables",
    "async_drop_tables",
    "create_tables",
    "drop_tables",
]
