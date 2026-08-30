# src/rhosocial/stateflow/models/order/model.py
"""Order model."""

import uuid
from datetime import datetime, timezone
from typing import Any, ClassVar, Optional, Sequence

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from rhosocial.stateflow.types import ORDER_STATUS_COMPLETED, ORDER_STATUS_PENDING


class Order(UUIDMixin, TimestampMixin, ActiveRecord):
    """Runtime process instance carrying business context and overall status."""

    __table_name__ = "stateflow_orders"

    template_id: uuid.UUID
    status: str = ORDER_STATUS_PENDING
    context: dict = Field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def mark_completed(self) -> None:
        """Mark the order as completed at the current UTC time."""
        self.status = ORDER_STATUS_COMPLETED
        self.completed_at = datetime.now(timezone.utc)

    def all_subprocesses_completed(self, subprocesses: Sequence[Any]) -> bool:
        """Return whether all non-skipped subprocesses reached advance states."""
        active_subprocesses = [subprocess for subprocess in subprocesses if not subprocess.skipped]
        return bool(active_subprocesses) and all(
            subprocess.is_advance_status() for subprocess in active_subprocesses
        )


class AsyncOrder(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    """Async sibling of :class:`Order` — same table, async I/O."""

    __table_name__ = "stateflow_orders"

    template_id: uuid.UUID
    status: str = ORDER_STATUS_PENDING
    context: dict = Field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def mark_completed(self) -> None:
        """Mark the order as completed at the current UTC time."""
        self.status = ORDER_STATUS_COMPLETED
        self.completed_at = datetime.now(timezone.utc)

    def all_subprocesses_completed(self, subprocesses: Sequence[Any]) -> bool:
        """Return whether all non-skipped subprocesses reached advance states."""
        active_subprocesses = [subprocess for subprocess in subprocesses if not subprocess.skipped]
        return bool(active_subprocesses) and all(
            subprocess.is_advance_status() for subprocess in active_subprocesses
        )
