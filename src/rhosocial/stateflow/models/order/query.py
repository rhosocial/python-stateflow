# src/rhosocial/stateflow/models/order/query.py
"""Query helpers for Order."""

from ...types import ORDER_STATUS_COMPLETED, ORDER_STATUS_PENDING, ORDER_STATUS_RUNNING
from .model import Order


class OrderQuery:
    """Query helpers for Order."""

    model = Order

    @classmethod
    def by_template_id(cls, template_id):
        return cls.model.query().where(cls.model.c.template_id == template_id)

    @classmethod
    def by_status(cls, status):
        return cls.model.query().where(cls.model.c.status == status)

    @classmethod
    def pending(cls):
        return cls.by_status(ORDER_STATUS_PENDING)

    @classmethod
    def running(cls):
        return cls.by_status(ORDER_STATUS_RUNNING)

    @classmethod
    def completed(cls):
        return cls.by_status(ORDER_STATUS_COMPLETED)

    @classmethod
    def started(cls):
        return cls.model.query().where(cls.model.c.started_at != None)

    @classmethod
    def completed_after(cls, moment):
        return cls.model.query().where(cls.model.c.completed_at >= moment)
