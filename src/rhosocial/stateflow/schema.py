# src/rhosocial/stateflow/schema.py
"""Schema helpers for stateflow tables using ActiveRecord DDL expressions.

All helpers are grouped in the :class:`Schema` namespace — no module-level
functions. Uses ``CreateTableExpression`` so the same definition compiles to
correct DDL for every supported backend (SQLite, MySQL, PostgreSQL, MariaDB,
etc.) via the backend's dialect.

Note: expression imports are deferred to method call time to avoid the ~10s
import cost of the expression module at package load.
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


class Schema:
    """Schema creation and teardown for stateflow tables.

    All methods are static — callers use ``Schema.create_tables(backend)``
    without instantiating. The class serves as a namespace that keeps
    DDL helpers out of the module-level scope.
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

    # ------------------------------------------------------------------
    # Column spec helpers (expression imports are deferred)
    # ------------------------------------------------------------------

    @staticmethod
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

    @staticmethod
    def _table(dialect, name, columns):
        """Build a CREATE TABLE IF NOT EXISTS expression."""
        from rhosocial.activerecord.backend.expression import CreateTableExpression
        return CreateTableExpression(
            dialect=dialect, table=name, if_not_exists=True, columns=columns,
        )

    @classmethod
    def _table_expressions(cls, dialect):
        """Return all 9 CreateTableExpression objects compiled for ``dialect``."""
        from rhosocial.activerecord.backend.expression.types import BooleanType, IntegerType, TextType, VarCharType

        _T = TextType()
        _I = IntegerType()
        _B = BooleanType()
        _ID = VarCharType(36)

        return [
            cls._table(dialect, "stateflow_order_templates", [
                cls._col("id", _ID, pk=True),
                cls._col("name", _T, not_null=True),
                cls._col("version", _I, not_null=True),
                cls._col("status", _T, not_null=True),
                cls._col("description", _T),
                cls._col("published_at", _T),
                cls._col("deprecated_at", _T),
                cls._col("created_by", _T),
                cls._col("checksum", _T),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_template_steps", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _T, not_null=True),
                cls._col("name", _T, not_null=True),
                cls._col("handler_class", _T, not_null=True),
                cls._col("terminal_states", _T, not_null=True),
                cls._col("advance_states", _T, not_null=True),
                cls._col("rollback_states", _T, not_null=True),
                cls._col("timeout_seconds", _I),
                cls._col("timeout_status", _T),
                cls._col("on_start_notify", _T),
                cls._col("on_complete_notify", _T),
                cls._col("on_rollback_notify", _T),
                cls._col("on_timeout_notify", _T),
                cls._col("depends_on", _T, not_null=True),
                cls._col("step_order", _I, not_null=True),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_flow_paths", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _T, not_null=True),
                cls._col("name", _T, not_null=True),
                cls._col("skip_steps", _T, not_null=True),
                cls._col("start_from", _T),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_orders", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _T, not_null=True),
                cls._col("status", _T, not_null=True),
                cls._col("context", _T, not_null=True),
                cls._col("started_at", _T),
                cls._col("completed_at", _T),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_processes", [
                cls._col("id", _ID, pk=True),
                cls._col("order_id", _T, not_null=True),
                cls._col("template_snapshot", _T, not_null=True),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_subprocesses", [
                cls._col("id", _ID, pk=True),
                cls._col("process_id", _T, not_null=True),
                cls._col("step_name", _T, not_null=True),
                cls._col("status", _T, not_null=True),
                cls._col("handler_class", _T, not_null=True),
                cls._col("terminal_states", _T, not_null=True),
                cls._col("advance_states", _T, not_null=True),
                cls._col("rollback_states", _T, not_null=True),
                cls._col("timeout_seconds", _I),
                cls._col("timeout_status", _T),
                cls._col("started_at", _T),
                cls._col("timeout_at", _T),
                cls._col("completed_at", _T),
                cls._col("skipped", _B, not_null=True),
                cls._col("extra", _T, not_null=True),
                cls._col("source", _T, not_null=True),
                cls._col("sequence", _I, not_null=True),
                cls._col("created_event_id", _T),
                cls._col("is_reversible", _B, not_null=True),
                cls._col("rollback_status", _T, not_null=True),
                cls._col("rollback_started_at", _T),
                cls._col("rollback_completed_at", _T),
                cls._col("rollback_error", _T),
                cls._col("version", _I, not_null=True),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_subprocess_dependencies", [
                cls._col("id", _ID, pk=True),
                cls._col("process_id", _T, not_null=True),
                cls._col("subprocess_id", _T, not_null=True),
                cls._col("depends_on_id", _T, not_null=True),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_events", [
                cls._col("id", _ID, pk=True),
                cls._col("order_id", _T, not_null=True),
                cls._col("subprocess_id", _T),
                cls._col("event_type", _T, not_null=True),
                cls._col("from_status", _T),
                cls._col("to_status", _T),
                cls._col("payload", _T, not_null=True),
                cls._col("event_key", _T),
                cls._col("correlation_id", _T),
                cls._col("causation_id", _T),
                cls._col("conflict", _B, not_null=True),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_outbox", [
                cls._col("id", _ID, pk=True),
                cls._col("event_id", _T, not_null=True),
                cls._col("topic", _T, not_null=True),
                cls._col("payload", _T, not_null=True),
                cls._col("status", _T, not_null=True),
                cls._col("retry_count", _I, not_null=True),
                cls._col("next_retry_at", _T),
                cls._col("created_at", _T, not_null=True),
                cls._col("updated_at", _T, not_null=True),
            ]),
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def create_tables(cls, backend, *, ddl=None) -> None:
        """Create all stateflow tables on the given backend.

        Uses the backend's dialect to compile ``CreateTableExpression`` objects
        into backend-specific DDL. Works for any supported backend (SQLite, MySQL,
        PostgreSQL, MariaDB, etc.) without per-backend SQL files.
        """
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        dialect = backend.dialect
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for expr in cls._table_expressions(dialect):
            sql, _ = expr.to_sql()
            backend.execute(sql, options=options)

    @classmethod
    def drop_tables(cls, backend) -> None:
        """Drop all stateflow tables."""
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for model in reversed(cls.ALL_MODELS):
            backend.execute(
                f"DROP TABLE IF EXISTS {model.__table_name__}",
                options=options,
            )

    @classmethod
    async def async_create_tables(cls, backend, *, ddl=None) -> None:
        """Create all stateflow tables on the given async backend."""
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        dialect = backend.dialect
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for expr in cls._table_expressions(dialect):
            sql, _ = expr.to_sql()
            await backend.execute(sql, options=options)

    @classmethod
    async def async_drop_tables(cls, backend) -> None:
        """Drop all stateflow tables on the given async backend."""
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for model in reversed(cls.ALL_MODELS):
            await backend.execute(
                f"DROP TABLE IF EXISTS {model.__table_name__}",
                options=options,
            )


__all__ = [
    "Schema",
]