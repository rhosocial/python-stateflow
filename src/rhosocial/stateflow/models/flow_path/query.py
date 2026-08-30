# src/rhosocial/stateflow/models/flow_path/query.py
"""Query helpers for FlowPath."""

from rhosocial.stateflow.models.flow_path.model import AsyncFlowPath, FlowPath

class _FlowPathQueryBase:
    """Shared query building logic for FlowPath and AsyncFlowPath siblings."""

    model = None


    @classmethod
    def by_template_id(cls, template_id):
        return cls.model.query().where(cls.model.c.template_id == template_id)

    @classmethod
    def by_name(cls, template_id, name):
        return cls.by_template_id(template_id).where(cls.model.c.name == name)

    @classmethod
    def with_start_from(cls, template_id=None):
        query = cls.model.query().where(cls.model.c.start_from != None)  # noqa: E711
        if template_id is not None:
            query = query.where(cls.model.c.template_id == template_id)
        return query

class FlowPathQuery(_FlowPathQueryBase):
    """Query helpers for FlowPath."""

    model = FlowPath


class AsyncFlowPathQuery(_FlowPathQueryBase):
    """Async query helpers for AsyncFlowPath."""

    model = AsyncFlowPath
