# src/rhosocial/stateflow/models/order_outbox/model.py
"""Order outbox model."""

import uuid
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord

from ...types import OUTBOX_STATUS_PENDING, OUTBOX_TOPIC_HANDLER_START


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

    @classmethod
    def handler_start(cls, event: object, subprocess: object) -> "OrderOutbox":
        """Build an outbox item for starting a subprocess handler."""
        return cls(
            event_id=event.id,
            topic=OUTBOX_TOPIC_HANDLER_START,
            payload={"subprocess_id": str(subprocess.id)},
        )
