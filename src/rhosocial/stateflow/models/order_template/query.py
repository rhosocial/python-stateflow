# src/rhosocial/stateflow/models/order_template/query.py
"""Query helpers for OrderTemplate."""

from ...types import TEMPLATE_STATUS_DRAFT, TEMPLATE_STATUS_PUBLISHED
from .model import OrderTemplate


class OrderTemplateQuery:
    """Query helpers for OrderTemplate."""

    model = OrderTemplate

    @classmethod
    def by_name(cls, name):
        return cls.model.query().where(cls.model.c.name == name)

    @classmethod
    def by_status(cls, status):
        return cls.model.query().where(cls.model.c.status == status)

    @classmethod
    def draft(cls):
        return cls.by_status(TEMPLATE_STATUS_DRAFT)

    @classmethod
    def published(cls):
        return cls.by_status(TEMPLATE_STATUS_PUBLISHED)

    @classmethod
    def by_name_version(cls, name, version):
        return cls.by_name(name).where(cls.model.c.version == version)
