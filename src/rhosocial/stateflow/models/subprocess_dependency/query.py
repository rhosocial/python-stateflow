# src/rhosocial/stateflow/models/subprocess_dependency/query.py
"""Query helpers for SubProcessDependency."""

from rhosocial.stateflow.models.subprocess_dependency.model import AsyncSubProcessDependency, SubProcessDependency

class _SubProcessDependencyQueryBase:
    """Shared query building logic for SubProcessDependency and AsyncSubProcessDependency siblings."""

    model = None


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

class SubProcessDependencyQuery(_SubProcessDependencyQueryBase):
    """Query helpers for SubProcessDependency."""

    model = SubProcessDependency


class AsyncSubProcessDependencyQuery(_SubProcessDependencyQueryBase):
    """Async query helpers for AsyncSubProcessDependency."""

    model = AsyncSubProcessDependency
