# src/rhosocial/stateflow/models/order_event/query.py
"""Query helpers for OrderEvent."""

from rhosocial.stateflow.models.order_event.model import AsyncOrderEvent, OrderEvent

class _OrderEventQueryBase:
    """Shared query building logic for OrderEvent and AsyncOrderEvent siblings."""

    model = None


    @classmethod
    def by_order_id(cls, order_id):
        return cls.model.query().where(cls.model.c.order_id == order_id)

    @classmethod
    def by_subprocess_id(cls, subprocess_id):
        return cls.model.query().where(cls.model.c.subprocess_id == subprocess_id)

    @classmethod
    def by_event_type(cls, event_type):
        return cls.model.query().where(cls.model.c.event_type == event_type)

    @classmethod
    def by_event_key(cls, event_key):
        return cls.model.query().where(cls.model.c.event_key == event_key)

    @classmethod
    def find_by_event_key(cls, event_key):
        return cls.by_event_key(event_key).one()

    @classmethod
    def conflicts(cls):
        return cls.model.query().where(cls.model.c.conflict == True)  # noqa: E712

    @classmethod
    def by_correlation_id(cls, correlation_id):
        return cls.model.query().where(cls.model.c.correlation_id == correlation_id)

    @classmethod
    def caused_by(cls, causation_id):
        return cls.model.query().where(cls.model.c.causation_id == causation_id)

class OrderEventQuery(_OrderEventQueryBase):
    """Query helpers for OrderEvent."""

    model = OrderEvent


class AsyncOrderEventQuery(_OrderEventQueryBase):
    """Async query helpers for AsyncOrderEvent."""

    model = AsyncOrderEvent
