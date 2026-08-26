# src/rhosocial/stateflow/models/order_template_step/query.py
"""Query helpers for OrderTemplateStep."""

from .model import OrderTemplateStep


class OrderTemplateStepQuery:
    """Query helpers for OrderTemplateStep."""

    model = OrderTemplateStep

    @classmethod
    def by_template_id(cls, template_id):
        return cls.model.query().where(cls.model.c.template_id == template_id)

    @classmethod
    def ordered(cls, template_id):
        return cls.by_template_id(template_id).order_by(cls.model.c.step_order)

    @classmethod
    def by_name(cls, template_id, name):
        return cls.by_template_id(template_id).where(cls.model.c.name == name)

    @classmethod
    def with_timeout(cls, template_id=None):
        query = cls.model.query().where(cls.model.c.timeout_seconds != None)  # noqa: E711
        if template_id is not None:
            query = query.where(cls.model.c.template_id == template_id)
        return query
