# src/rhosocial/stateflow/models/order_process/query.py
"""Query helpers for OrderProcess."""

from rhosocial.stateflow.models.order_process.model import AsyncOrderProcess, OrderProcess

class _OrderProcessQueryBase:
    """Shared query building logic for OrderProcess and AsyncOrderProcess siblings."""

    model = None


    @classmethod
    def by_order_id(cls, order_id):
        return cls.model.query().where(cls.model.c.order_id == order_id)

    @classmethod
    def find_by_order_id(cls, order_id):
        return cls.by_order_id(order_id).one()

class OrderProcessQuery(_OrderProcessQueryBase):
    """Query helpers for OrderProcess."""

    model = OrderProcess


class AsyncOrderProcessQuery(_OrderProcessQueryBase):
    """Async query helpers for AsyncOrderProcess."""

    model = AsyncOrderProcess
