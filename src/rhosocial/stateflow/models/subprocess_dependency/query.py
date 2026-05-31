# src/rhosocial/stateflow/models/subprocess_dependency/query.py
"""Query helpers for SubProcessDependency."""

from .model import SubProcessDependency


class SubProcessDependencyQuery:
    """Query helpers for SubProcessDependency."""

    model = SubProcessDependency

    @classmethod
    def by_process_id(cls, process_id):
        return cls.model.query().where(cls.model.c.process_id == process_id)

    @classmethod
    def for_subprocess(cls, subprocess_id):
        return cls.model.query().where(cls.model.c.subprocess_id == subprocess_id)

    @classmethod
    def depending_on(cls, depends_on_id):
        return cls.model.query().where(cls.model.c.depends_on_id == depends_on_id)

    @classmethod
    def between(cls, subprocess_id, depends_on_id):
        return cls.for_subprocess(subprocess_id).where(cls.model.c.depends_on_id == depends_on_id)
