# src/rhosocial/stateflow/models.py
"""ActiveRecord models for stateflow."""

import uuid
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import OptimisticLockMixin, TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord

from .types import (
    ORDER_STATUS_PENDING,
    OUTBOX_STATUS_PENDING,
    ROLLBACK_STATUS_NOT_REQUIRED,
    SUBPROCESS_SOURCE_TEMPLATE,
    SUBPROCESS_STATUS_PENDING,
    TEMPLATE_STATUS_DRAFT,
)


class OrderTemplate(UUIDMixin, TimestampMixin, ActiveRecord):
    """Process blueprint that should evolve through new versions after publication."""

    __table_name__ = "stateflow_order_templates"

    name: str
    version: int = 1
    status: str = TEMPLATE_STATUS_DRAFT
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    deprecated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    checksum: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()


class OrderTemplateStep(UUIDMixin, TimestampMixin, ActiveRecord):
    """Subprocess definition with dependency and state classification rules."""

    __table_name__ = "stateflow_order_template_steps"

    template_id: uuid.UUID
    name: str
    handler_class: str
    terminal_states: List[str] = Field(default_factory=list)
    advance_states: List[str] = Field(default_factory=list)
    rollback_states: List[str] = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    timeout_status: Optional[str] = None
    on_start_notify: Optional[Dict[str, Any]] = None
    on_complete_notify: Optional[Dict[str, Any]] = None
    on_rollback_notify: Optional[Dict[str, Any]] = None
    on_timeout_notify: Optional[Dict[str, Any]] = None
    depends_on: List[str] = Field(default_factory=list)
    step_order: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()


class FlowPath(UUIDMixin, TimestampMixin, ActiveRecord):
    """Path variant declaring an entry step and skipped steps."""

    __table_name__ = "stateflow_flow_paths"

    template_id: uuid.UUID
    name: str
    skip_steps: List[str] = Field(default_factory=list)
    start_from: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()


class Order(UUIDMixin, TimestampMixin, ActiveRecord):
    """Runtime process instance carrying business context and overall status."""

    __table_name__ = "stateflow_orders"

    template_id: uuid.UUID
    status: str = ORDER_STATUS_PENDING
    context: Dict[str, Any] = Field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    c: ClassVar[FieldProxy] = FieldProxy()


class OrderProcess(UUIDMixin, TimestampMixin, ActiveRecord):
    """Template snapshot bound to an order so running instances stay stable."""

    __table_name__ = "stateflow_order_processes"

    order_id: uuid.UUID
    template_snapshot: Dict[str, Any] = Field(default_factory=dict)

    c: ClassVar[FieldProxy] = FieldProxy()


class OrderSubProcess(UUIDMixin, TimestampMixin, OptimisticLockMixin, ActiveRecord):
    """Runtime state for one subprocess with optimistic concurrency control."""

    __table_name__ = "stateflow_order_subprocesses"

    process_id: uuid.UUID
    step_name: str
    status: str = SUBPROCESS_STATUS_PENDING
    handler_class: str
    terminal_states: List[str] = Field(default_factory=list)
    advance_states: List[str] = Field(default_factory=list)
    rollback_states: List[str] = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    timeout_status: Optional[str] = None
    started_at: Optional[datetime] = None
    timeout_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    skipped: bool = False
    extra: Dict[str, Any] = Field(default_factory=dict)
    source: str = SUBPROCESS_SOURCE_TEMPLATE
    sequence: int = 0
    created_event_id: Optional[uuid.UUID] = None
    is_reversible: bool = False
    rollback_status: str = ROLLBACK_STATUS_NOT_REQUIRED
    rollback_started_at: Optional[datetime] = None
    rollback_completed_at: Optional[datetime] = None
    rollback_error: Optional[Dict[str, Any]] = None

    c: ClassVar[FieldProxy] = FieldProxy()


class SubProcessDependency(UUIDMixin, TimestampMixin, ActiveRecord):
    """Dependency edge from a subprocess to one upstream subprocess."""

    __table_name__ = "stateflow_subprocess_dependencies"

    process_id: uuid.UUID
    subprocess_id: uuid.UUID
    depends_on_id: uuid.UUID

    c: ClassVar[FieldProxy] = FieldProxy()


class OrderEvent(UUIDMixin, TimestampMixin, ActiveRecord):
    """Immutable event log entry for auditing, idempotency, and causality."""

    __table_name__ = "stateflow_order_events"

    order_id: uuid.UUID
    subprocess_id: Optional[uuid.UUID] = None
    event_type: str
    from_status: Optional[str] = None
    to_status: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    event_key: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[uuid.UUID] = None
    conflict: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()


class OrderOutbox(UUIDMixin, TimestampMixin, ActiveRecord):
    """Side-effect delivery record decoupling state changes from external calls."""

    __table_name__ = "stateflow_order_outbox"

    event_id: uuid.UUID
    topic: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: str = OUTBOX_STATUS_PENDING
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None

    c: ClassVar[FieldProxy] = FieldProxy()
