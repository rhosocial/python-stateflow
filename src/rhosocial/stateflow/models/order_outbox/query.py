# src/rhosocial/stateflow/models/order_outbox/query.py
"""Query helpers for OrderOutbox."""

from rhosocial.stateflow.types import OUTBOX_STATUS_FAILED, OUTBOX_STATUS_PENDING, OUTBOX_STATUS_PROCESSING
from rhosocial.stateflow.models.order_outbox.model import AsyncOrderOutbox, OrderOutbox

class _OrderOutboxQueryBase:
    """Shared query building logic for OrderOutbox and AsyncOrderOutbox siblings."""

    model = None


    @classmethod
    def by_status(cls, status):
        return cls.model.query().where(cls.model.c.status == status)

    @classmethod
    def pending(cls):
        return cls.by_status(OUTBOX_STATUS_PENDING)

    @classmethod
    def processing(cls):
        return cls.by_status(OUTBOX_STATUS_PROCESSING)

    @classmethod
    def failed(cls):
        return cls.by_status(OUTBOX_STATUS_FAILED)

    @classmethod
    def by_topic(cls, topic):
        return cls.model.query().where(cls.model.c.topic == topic)

    @classmethod
    def by_event_id(cls, event_id):
        return cls.model.query().where(cls.model.c.event_id == event_id)

    @classmethod
    def due_for_retry(cls, moment):
        return cls.failed().where(cls.model.c.next_retry_at <= moment)

class OrderOutboxQuery(_OrderOutboxQueryBase):
    """Query helpers for OrderOutbox."""

    model = OrderOutbox


class AsyncOrderOutboxQuery(_OrderOutboxQueryBase):
    """Async query helpers for AsyncOrderOutbox."""

    model = AsyncOrderOutbox
