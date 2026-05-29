# src/rhosocial/stateflow/types.py
"""Shared types for stateflow."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

ORDER_STATUS_PENDING = "pending"
ORDER_STATUS_RUNNING = "running"
ORDER_STATUS_COMPLETED = "completed"
ORDER_STATUS_ROLLED_BACK = "rolled_back"
ORDER_STATUS_SUSPENDED = "suspended"

TEMPLATE_STATUS_DRAFT = "draft"
TEMPLATE_STATUS_PUBLISHED = "published"
TEMPLATE_STATUS_DEPRECATED = "deprecated"
TEMPLATE_STATUS_ARCHIVED = "archived"

SUBPROCESS_STATUS_PENDING = "pending"
SUBPROCESS_STATUS_RUNNING = "running"

SUBPROCESS_SOURCE_TEMPLATE = "template"
SUBPROCESS_SOURCE_DYNAMIC = "dynamic"

ROLLBACK_STATUS_NOT_REQUIRED = "not_required"
ROLLBACK_STATUS_PENDING = "pending"
ROLLBACK_STATUS_RUNNING = "running"
ROLLBACK_STATUS_COMPLETED = "completed"
ROLLBACK_STATUS_FAILED = "failed"

EVENT_ORDER_CREATED = "order_created"
EVENT_ORDER_COMPLETED = "order_completed"
EVENT_SP_CREATED = "sp_created"
EVENT_SP_SKIPPED = "sp_skipped"
EVENT_SP_STARTED = "sp_started"
EVENT_SP_STATUS_CHANGED = "sp_status_changed"
EVENT_SP_APPENDED = "sp_appended"
EVENT_SP_TIMEOUT = "sp_timeout"
EVENT_CONFLICT = "conflict"

OUTBOX_STATUS_PENDING = "pending"
OUTBOX_STATUS_PROCESSING = "processing"
OUTBOX_STATUS_SENT = "sent"
OUTBOX_STATUS_FAILED = "failed"
OUTBOX_STATUS_CANCELLED = "cancelled"

OUTBOX_TOPIC_HANDLER_START = "handler_start"
OUTBOX_TOPIC_HANDLER_ROLLBACK = "handler_rollback"
OUTBOX_TOPIC_NOTIFICATION = "notification"
OUTBOX_TOPIC_TIMER = "timer"


@dataclass
class HandlerResult:
    """Terminal or intermediate status reported by a subprocess handler."""

    status: str
    payload: Optional[Dict[str, Any]] = None
    event_key: Optional[str] = None


@dataclass
class ValidationIssue:
    """Single validation issue with a stable code and optional path."""

    code: str
    message: str
    path: Optional[str] = None


@dataclass
class ValidationResult:
    """Aggregates validation issues while keeping validation side-effect free."""

    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.issues

    def add(self, code: str, message: str, path: Optional[str] = None) -> None:
        self.issues.append(ValidationIssue(code=code, message=message, path=path))
