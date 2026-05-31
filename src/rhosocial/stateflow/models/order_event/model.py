# src/rhosocial/stateflow/models/order_event/model.py
"""Order event model."""

import uuid
from typing import Any, ClassVar, Dict, Optional, Sequence

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord

from ...types import (
    EVENT_CONFLICT,
    EVENT_ORDER_COMPLETED,
    EVENT_ORDER_CREATED,
    EVENT_SP_CREATED,
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
    payload: Dict[str, Any] = Field(default_factory=dict)
    event_key: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[uuid.UUID] = None
    conflict: bool = False

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def order_created(cls, order: object) -> "OrderEvent":
        """Build an order-created event."""
        return cls(order_id=order.id, event_type=EVENT_ORDER_CREATED)

    @classmethod
    def subprocess_created(cls, order: object, subprocess: object) -> "OrderEvent":
        """Build a subprocess-created event."""
        return cls(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_CREATED,
            to_status=subprocess.status,
            payload={"step_name": subprocess.step_name},
        )

    @classmethod
    def subprocess_skipped(cls, order: object, subprocess: object) -> "OrderEvent":
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
        order: object,
        subprocess: object,
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
        order: object,
        subprocess: object,
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
    def order_completed(cls, order: object) -> "OrderEvent":
        """Build an order-completed event."""
        return cls(
            order_id=order.id,
            event_type=EVENT_ORDER_COMPLETED,
            to_status=ORDER_STATUS_COMPLETED,
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
