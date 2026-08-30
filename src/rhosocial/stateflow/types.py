# src/rhosocial/stateflow/types.py
"""Shared types for stateflow.

All framework-reserved string tags (status values, event types, outbox
topics, sources) are **namespaced** so they can never collide with
user-declared business values (e.g. a custom ``terminal_state`` named
``"running"``) or with tags from other frameworks. The ``stateflow:``
prefix is the reserved namespace for this package.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Order lifecycle
# ---------------------------------------------------------------------------

ORDER_STATUS_PENDING = "stateflow:order:pending"
ORDER_STATUS_RUNNING = "stateflow:order:running"
ORDER_STATUS_COMPLETED = "stateflow:order:completed"
ORDER_STATUS_ROLLED_BACK = "stateflow:order:rolled_back"
ORDER_STATUS_SUSPENDED = "stateflow:order:suspended"

# ---------------------------------------------------------------------------
# Template lifecycle
# ---------------------------------------------------------------------------

TEMPLATE_STATUS_DRAFT = "stateflow:template:draft"
TEMPLATE_STATUS_PUBLISHED = "stateflow:template:published"
TEMPLATE_STATUS_DEPRECATED = "stateflow:template:deprecated"
TEMPLATE_STATUS_ARCHIVED = "stateflow:template:archived"

# ---------------------------------------------------------------------------
# Subprocess lifecycle (framework-reserved states only; user-declared
# terminal/advance/rollback states are never namespaced)
# ---------------------------------------------------------------------------

SUBPROCESS_STATUS_PENDING = "stateflow:subprocess:pending"
SUBPROCESS_STATUS_RUNNING = "stateflow:subprocess:running"

SUBPROCESS_SOURCE_TEMPLATE = "stateflow:source:template"
SUBPROCESS_SOURCE_DYNAMIC = "stateflow:source:dynamic"

# ---------------------------------------------------------------------------
# Rollback lifecycle
# ---------------------------------------------------------------------------

ROLLBACK_STATUS_NOT_REQUIRED = "stateflow:rollback:not_required"
ROLLBACK_STATUS_PENDING = "stateflow:rollback:pending"
ROLLBACK_STATUS_RUNNING = "stateflow:rollback:running"
ROLLBACK_STATUS_COMPLETED = "stateflow:rollback:completed"
ROLLBACK_STATUS_FAILED = "stateflow:rollback:failed"

# ---------------------------------------------------------------------------
# Event types (immutable event log discriminators)
# ---------------------------------------------------------------------------

EVENT_ORDER_CREATED = "stateflow:event:order_created"
EVENT_ORDER_COMPLETED = "stateflow:event:order_completed"
EVENT_SP_CREATED = "stateflow:event:sp_created"
EVENT_SP_SKIPPED = "stateflow:event:sp_skipped"
EVENT_SP_STARTED = "stateflow:event:sp_started"
EVENT_SP_STATUS_CHANGED = "stateflow:event:sp_status_changed"
EVENT_SP_APPENDED = "stateflow:event:sp_appended"
EVENT_SP_TIMEOUT = "stateflow:event:sp_timeout"
EVENT_SP_ROLLBACK_STARTED = "stateflow:event:sp_rollback_started"
EVENT_SP_ROLLBACK_COMPLETED = "stateflow:event:sp_rollback_completed"
EVENT_SP_ROLLBACK_FAILED = "stateflow:event:sp_rollback_failed"
EVENT_CONFLICT = "stateflow:event:conflict"

# ---------------------------------------------------------------------------
# Outbox lifecycle
# ---------------------------------------------------------------------------

OUTBOX_STATUS_PENDING = "stateflow:outbox:pending"
OUTBOX_STATUS_PROCESSING = "stateflow:outbox:processing"
OUTBOX_STATUS_SENT = "stateflow:outbox:sent"
OUTBOX_STATUS_FAILED = "stateflow:outbox:failed"
OUTBOX_STATUS_CANCELLED = "stateflow:outbox:cancelled"

OUTBOX_TOPIC_HANDLER_START = "stateflow:topic:handler_start"
OUTBOX_TOPIC_HANDLER_ROLLBACK = "stateflow:topic:handler_rollback"
OUTBOX_TOPIC_NOTIFICATION = "stateflow:topic:notification"
OUTBOX_TOPIC_TIMER = "stateflow:topic:timer"


@dataclass
class HandlerResult:
    """Terminal or intermediate status reported by a subprocess handler.

    ``status`` and ``event_key`` are opaque strings: ``status`` is a
    user-declared subprocess state (never namespaced), ``event_key`` is a
    caller-supplied idempotency key.
    """

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
