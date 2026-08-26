# src/rhosocial/stateflow/models/order_event/model.py
"""Order event model."""

import uuid
from typing import Any, ClassVar, Dict, Optional, Sequence

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from ...types import (
    EVENT_CONFLICT,
    EVENT_ORDER_COMPLETED,
    EVENT_ORDER_CREATED,
    EVENT_SP_CREATED,
    EVENT_SP_ROLLBACK_COMPLETED,
    EVENT_SP_ROLLBACK_FAILED,
    EVENT_SP_ROLLBACK_STARTED,
    EVENT_SP_SKIPPED,
    EVENT_SP_STATUS_CHANGED,
    ORDER_STATUS_COMPLETED,
)


class OrderEvent(UUIDMixin, TimestampMixin, ActiveRecord):
    """Immutable event log entry for auditing, idempotency, and causality."""

    __table_name__ = "stateflow_order_events"

    order_id: uuid.UUID
    subprocess_id: Optional[uuid.UUID] = None
    event_type: str
    from_status: Optional[str] = None
    to_status: Optional[str] = None
    payload: dict = Field(default_factory=dict)
    event_key: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[uuid.UUID] = None
    conflict: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def order_created(cls, order: Any) -> "OrderEvent":
        """Build an order-created event."""
        return cls(order_id=order.id, event_type=EVENT_ORDER_CREATED)

    @classmethod
    def subprocess_created(cls, order: Any, subprocess: Any) -> "OrderEvent":
        """Build a subprocess-created event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_CREATED,
            to_status=subprocess.status,
            payload={"step_name": subprocess.step_name},
        )

    @classmethod
    def subprocess_skipped(cls, order: Any, subprocess: Any) -> "OrderEvent":
        """Build a subprocess-skipped event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_SKIPPED,
            to_status=subprocess.status,
            payload={"step_name": subprocess.step_name},
        )

    @classmethod
    def status_changed(
        cls,
        order: Any,
        subprocess: Any,
        previous_status: str,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> "OrderEvent":
        """Build a subprocess status-changed event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_STATUS_CHANGED,
            from_status=previous_status,
            to_status=new_status,
            payload=payload or {},
            event_key=event_key,
        )

    @classmethod
    def conflict_event(
        cls,
        order: Any,
        subprocess: Any,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> "OrderEvent":
        """Build a conflict event for an attempted terminal overwrite."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_CONFLICT,
            from_status=subprocess.status,
            to_status=new_status,
            payload=payload or {},
            event_key=event_key,
            conflict=True,
        )

    @classmethod
    def order_completed(cls, order: Any) -> "OrderEvent":
        """Build an order-completed event."""
        return cls(
            order_id=order.id,
            event_type=EVENT_ORDER_COMPLETED,
            to_status=ORDER_STATUS_COMPLETED,
        )

    @classmethod
    def rollback_started(
        cls,
        order: Any,
        subprocess: Any,
        event_key: Optional[str] = None,
    ) -> "OrderEvent":
        """Build a rollback-started event for a reversible subprocess."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_STARTED,
            from_status=subprocess.status,
            payload={"rollback_status": subprocess.rollback_status},
            event_key=event_key,
        )

    @classmethod
    def rollback_completed(
        cls,
        order: Any,
        subprocess: Any,
        new_status: str,
        payload: Optional[Dict] = None,
    ) -> "OrderEvent":
        """Build a rollback-completed event recording the post-rollback status."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_COMPLETED,
            from_status=subprocess.status,
            to_status=new_status,
            payload=payload or {},
        )

    @classmethod
    def rollback_failed(
        cls,
        order: Any,
        subprocess: Any,
        error: Optional[Dict] = None,
    ) -> "OrderEvent":
        """Build a rollback-failed event carrying the error payload."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_FAILED,
            from_status=subprocess.status,
            payload=error or {},
        )

    @classmethod
    def find_by_event_key(
        cls,
        events: Sequence["OrderEvent"],
        event_key: Optional[str],
    ) -> Optional["OrderEvent"]:
        """Return the first event matching an idempotency key."""
        if not event_key:
            return None
        for event in events:
            if event.event_key == event_key:
                return event
        return None


class AsyncOrderEvent(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    """Async sibling of :class:`OrderEvent`."""

    __table_name__ = "stateflow_order_events"

    order_id: uuid.UUID
    subprocess_id: Optional[uuid.UUID] = None
    event_type: str
    from_status: Optional[str] = None
    to_status: Optional[str] = None
    payload: dict = Field(default_factory=dict)
    event_key: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[uuid.UUID] = None
    conflict: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def order_created(cls, order: Any) -> "AsyncOrderEvent":
        """Build an order-created event."""
        return cls(order_id=order.id, event_type=EVENT_ORDER_CREATED)

    @classmethod
    def subprocess_created(cls, order: Any, subprocess: Any) -> "AsyncOrderEvent":
        """Build a subprocess-created event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_CREATED,
            to_status=subprocess.status,
            payload={"step_name": subprocess.step_name},
        )

    @classmethod
    def subprocess_skipped(cls, order: Any, subprocess: Any) -> "AsyncOrderEvent":
        """Build a subprocess-skipped event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_SKIPPED,
            to_status=subprocess.status,
            payload={"step_name": subprocess.step_name},
        )

    @classmethod
    def status_changed(
        cls,
        order: Any,
        subprocess: Any,
        previous_status: str,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> "AsyncOrderEvent":
        """Build a subprocess status-changed event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_STATUS_CHANGED,
            from_status=previous_status,
            to_status=new_status,
            payload=payload or {},
            event_key=event_key,
        )

    @classmethod
    def conflict_event(
        cls,
        order: Any,
        subprocess: Any,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> "AsyncOrderEvent":
        """Build a conflict event for an attempted terminal overwrite."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_CONFLICT,
            from_status=subprocess.status,
            to_status=new_status,
            payload=payload or {},
            event_key=event_key,
            conflict=True,
        )

    @classmethod
    def order_completed(cls, order: Any) -> "AsyncOrderEvent":
        """Build an order-completed event."""
        return cls(
            order_id=order.id,
            event_type=EVENT_ORDER_COMPLETED,
            to_status=ORDER_STATUS_COMPLETED,
        )

    @classmethod
    def rollback_started(
        cls,
        order: Any,
        subprocess: Any,
        event_key: Optional[str] = None,
    ) -> "AsyncOrderEvent":
        """Build a rollback-started event for a reversible subprocess."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_STARTED,
            from_status=subprocess.status,
            payload={"rollback_status": subprocess.rollback_status},
            event_key=event_key,
        )

    @classmethod
    def rollback_completed(
        cls,
        order: Any,
        subprocess: Any,
        new_status: str,
        payload: Optional[Dict] = None,
    ) -> "AsyncOrderEvent":
        """Build a rollback-completed event recording the post-rollback status."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_COMPLETED,
            from_status=subprocess.status,
            to_status=new_status,
            payload=payload or {},
        )

    @classmethod
    def rollback_failed(
        cls,
        order: Any,
        subprocess: Any,
        error: Optional[Dict] = None,
    ) -> "AsyncOrderEvent":
        """Build a rollback-failed event carrying the error payload."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_ROLLBACK_FAILED,
            from_status=subprocess.status,
            payload=error or {},
        )

    @classmethod
    def find_by_event_key(
        cls,
        events: Sequence["AsyncOrderEvent"],
        event_key: Optional[str],
    ) -> Optional["AsyncOrderEvent"]:
        """Return the first event matching an idempotency key."""
        if not event_key:
            return None
        for event in events:
            if event.event_key == event_key:
                return event
        return None
