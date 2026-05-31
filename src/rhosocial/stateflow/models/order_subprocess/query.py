# src/rhosocial/stateflow/models/order_subprocess/query.py
"""Query helpers for OrderSubProcess."""

from ...types import (
    SUBPROCESS_SOURCE_DYNAMIC,
    SUBPROCESS_SOURCE_TEMPLATE,
    SUBPROCESS_STATUS_PENDING,
    SUBPROCESS_STATUS_RUNNING,
)
from .model import OrderSubProcess


class OrderSubProcessQuery:
    """Query helpers for OrderSubProcess."""

    model = OrderSubProcess

    @classmethod
    def by_process_id(cls, process_id):
        return cls.model.query().where(cls.model.c.process_id == process_id)

    @classmethod
    def by_step_name(cls, process_id, step_name):
        return cls.by_process_id(process_id).where(cls.model.c.step_name == step_name)

    @classmethod
    def by_status(cls, status):
        return cls.model.query().where(cls.model.c.status == status)

    @classmethod
    def pending(cls, process_id=None):
        query = cls.by_status(SUBPROCESS_STATUS_PENDING)
        if process_id is not None:
            query = query.where(cls.model.c.process_id == process_id)
        return query

    @classmethod
    def running(cls, process_id=None):
        query = cls.by_status(SUBPROCESS_STATUS_RUNNING)
        if process_id is not None:
            query = query.where(cls.model.c.process_id == process_id)
        return query

    @classmethod
    def not_skipped(cls, process_id=None):
        query = cls.model.query().where(cls.model.c.skipped == False)
        if process_id is not None:
            query = query.where(cls.model.c.process_id == process_id)
        return query

    @classmethod
    def skipped(cls, process_id=None):
        query = cls.model.query().where(cls.model.c.skipped == True)
        if process_id is not None:
            query = query.where(cls.model.c.process_id == process_id)
        return query

    @classmethod
    def by_source(cls, source):
        return cls.model.query().where(cls.model.c.source == source)

    @classmethod
    def template_source(cls):
        return cls.by_source(SUBPROCESS_SOURCE_TEMPLATE)

    @classmethod
    def dynamic_source(cls):
        return cls.by_source(SUBPROCESS_SOURCE_DYNAMIC)

    @classmethod
    def with_timeout(cls, process_id=None):
        query = cls.model.query().where(cls.model.c.timeout_seconds != None)
        if process_id is not None:
            query = query.where(cls.model.c.process_id == process_id)
        return query

    @classmethod
    def timeouts_due(cls, moment):
        return cls.not_skipped().where(cls.model.c.timeout_at <= moment)
