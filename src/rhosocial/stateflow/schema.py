# src/rhosocial/stateflow/schema.py
"""Schema helpers for stateflow tables using ActiveRecord DDL expressions.

All helpers are grouped in the :class:`Schema` namespace — no module-level
functions. Uses ``CreateTableExpression`` so the same definition compiles to
correct DDL for every supported backend (SQLite, MySQL, PostgreSQL, MariaDB,
etc.) via the backend's dialect.

Note: expression imports are deferred to method call time to avoid the ~10s
import cost of the expression module at package load.
"""

from rhosocial.stateflow.models.flow_path import FlowPath
from rhosocial.stateflow.models.order import Order
from rhosocial.stateflow.models.order_event import OrderEvent
from rhosocial.stateflow.models.order_outbox import OrderOutbox
from rhosocial.stateflow.models.order_process import OrderProcess
from rhosocial.stateflow.models.order_subprocess import OrderSubProcess
from rhosocial.stateflow.models.order_template import OrderTemplate
from rhosocial.stateflow.models.order_template_step import OrderTemplateStep
from rhosocial.stateflow.models.subprocess_dependency import SubProcessDependency


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
        from rhosocial.activerecord.backend.expression.types import (
            BooleanType, DateTimeType, IntegerType, TextType, VarCharType,
        )

        _T = TextType()
        _I = IntegerType()
        _B = BooleanType()
        _ID = VarCharType(36)
        _V = VarCharType(255)
        _S = VarCharType(64)
        # Timestamps: TEXT keeps the ISO-string round-trip (tz-aware
        # comparisons) on every backend; Oracle renders TEXT as CLOB, which
        # cannot appear in WHERE comparisons, and Firebird TEXT columns force
        # the driver to stringify datetime parameters — both need a real
        # TIMESTAMP instead.
        if getattr(dialect, "name", "").lower() in ("oracle", "firebird"):
            _D = DateTimeType()
        else:
            _D = TextType()

        return [
            cls._table(dialect, "stateflow_order_templates", [
                cls._col("id", _ID, pk=True),
                cls._col("name", _V, not_null=True),
                cls._col("version", _I, not_null=True),
                cls._col("status", _S, not_null=True),
                cls._col("description", _V),
                cls._col("published_at", _D),
                cls._col("deprecated_at", _D),
                cls._col("created_by", _V),
                cls._col("checksum", _V),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_template_steps", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _ID, not_null=True),
                cls._col("name", _V, not_null=True),
                cls._col("handler_class", _V, not_null=True),
                cls._col("terminal_states", _T, not_null=True),
                cls._col("advance_states", _T, not_null=True),
                cls._col("rollback_states", _T, not_null=True),
                cls._col("timeout_seconds", _I),
                cls._col("timeout_status", _S),
                cls._col("on_start_notify", _T),
                cls._col("on_complete_notify", _T),
                cls._col("on_rollback_notify", _T),
                cls._col("on_timeout_notify", _T),
                cls._col("depends_on", _T, not_null=True),
                cls._col("step_order", _I, not_null=True),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_flow_paths", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _ID, not_null=True),
                cls._col("name", _V, not_null=True),
                cls._col("skip_steps", _T, not_null=True),
                cls._col("start_from", _V),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_orders", [
                cls._col("id", _ID, pk=True),
                cls._col("template_id", _ID, not_null=True),
                cls._col("status", _S, not_null=True),
                cls._col("context", _T, not_null=True),
                cls._col("started_at", _D),
                cls._col("completed_at", _D),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_processes", [
                cls._col("id", _ID, pk=True),
                cls._col("order_id", _ID, not_null=True),
                cls._col("template_snapshot", _T, not_null=True),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_subprocesses", [
                cls._col("id", _ID, pk=True),
                cls._col("process_id", _ID, not_null=True),
                cls._col("step_name", _V, not_null=True),
                cls._col("status", _S, not_null=True),
                cls._col("handler_class", _V, not_null=True),
                cls._col("terminal_states", _T, not_null=True),
                cls._col("advance_states", _T, not_null=True),
                cls._col("rollback_states", _T, not_null=True),
                cls._col("timeout_seconds", _I),
                cls._col("timeout_status", _S),
                cls._col("started_at", _D),
                cls._col("timeout_at", _D),
                cls._col("completed_at", _D),
                cls._col("skipped", _B, not_null=True),
                cls._col("extra", _T, not_null=True),
                cls._col("source", _S, not_null=True),
                cls._col("sequence", _I, not_null=True),
                cls._col("created_event_id", _ID),
                cls._col("is_reversible", _B, not_null=True),
                cls._col("rollback_status", _S, not_null=True),
                cls._col("rollback_started_at", _D),
                cls._col("rollback_completed_at", _D),
                cls._col("rollback_error", _T),
                cls._col("version", _I, not_null=True),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_subprocess_dependencies", [
                cls._col("id", _ID, pk=True),
                cls._col("process_id", _ID, not_null=True),
                cls._col("subprocess_id", _ID, not_null=True),
                cls._col("depends_on_id", _ID, not_null=True),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_events", [
                cls._col("id", _ID, pk=True),
                cls._col("order_id", _ID, not_null=True),
                cls._col("subprocess_id", _ID),
                cls._col("event_type", _S, not_null=True),
                cls._col("from_status", _S),
                cls._col("to_status", _S),
                cls._col("payload", _T, not_null=True),
                cls._col("event_key", _V),
                cls._col("correlation_id", _ID),
                cls._col("causation_id", _ID),
                cls._col("conflict", _B, not_null=True),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
            ]),
            cls._table(dialect, "stateflow_order_outbox", [
                cls._col("id", _ID, pk=True),
                cls._col("event_id", _ID, not_null=True),
                cls._col("topic", _S, not_null=True),
                cls._col("payload", _T, not_null=True),
                cls._col("status", _S, not_null=True),
                cls._col("retry_count", _I, not_null=True),
                cls._col("next_retry_at", _D),
                cls._col("created_at", _D, not_null=True),
                cls._col("updated_at", _D, not_null=True),
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
    def _drop_table_sql(cls, dialect, table_name: str) -> str:
        """Build an idempotent DROP TABLE statement for *dialect*.

        Dialects supporting ``DROP TABLE IF EXISTS`` (Oracle 23c+, SQL Server
        2016+, MySQL/MariaDB/PostgreSQL/SQLite) take the plain form. Firebird
        supports neither IF EXISTS on DROP nor IF NOT EXISTS on CREATE, so the
        drop is wrapped in an EXECUTE BLOCK existence guard over RDB$RELATIONS.
        """
        if getattr(dialect, "name", "").lower() != "firebird":
            return f"DROP TABLE IF EXISTS {table_name}"
        return (
            "EXECUTE BLOCK AS DECLARE VARIABLE CNT INTEGER; BEGIN "
            "SELECT COUNT(*) FROM RDB$RELATIONS "
            f"WHERE TRIM(RDB$RELATION_NAME) = '{table_name.upper()}' INTO :CNT; "
            "IF (:CNT > 0) THEN "
            f"EXECUTE STATEMENT 'DROP TABLE {table_name}'; END"
        )

    @classmethod
    def drop_tables(cls, backend) -> None:
        """Drop all stateflow tables."""
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        dialect = backend.dialect
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for model in reversed(cls.ALL_MODELS):
            backend.execute(
                cls._drop_table_sql(dialect, model.__table_name__),
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

        dialect = backend.dialect
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        for model in reversed(cls.ALL_MODELS):
            await backend.execute(
                cls._drop_table_sql(dialect, model.__table_name__),
                options=options,
            )


__all__ = [
    "Schema",
]