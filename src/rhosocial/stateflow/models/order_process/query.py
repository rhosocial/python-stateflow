# src/rhosocial/stateflow/models/order_process/query.py
"""Query helpers for OrderProcess."""

from .model import OrderProcess


class OrderProcessQuery:
    """Query helpers for OrderProcess."""

    model = OrderProcess

    @classmethod
    def by_order_id(cls, order_id):
        return cls.model.query().where(cls.model.c.order_id == order_id)

    @classmethod
    def find_by_order_id(cls, order_id):
        return cls.by_order_id(order_id).one()
