# src/rhosocial/stateflow/models/order_process/model.py
"""Order process model."""

import uuid
from typing import Any, ClassVar, Dict, Sequence

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord


class OrderProcess(UUIDMixin, TimestampMixin, ActiveRecord):
    """Template snapshot bound to an order so running instances stay stable."""

    __table_name__ = "stateflow_order_processes"

    order_id: uuid.UUID
    template_snapshot: Dict[str, Any] = Field(default_factory=dict)

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def from_template(cls, order: object, template: object, steps: Sequence[object]) -> "OrderProcess":
        """Build a process from an order and a template snapshot."""
        return cls(order_id=order.id, template_snapshot=template.snapshot(steps))
