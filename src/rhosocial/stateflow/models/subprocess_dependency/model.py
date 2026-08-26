# src/rhosocial/stateflow/models/subprocess_dependency/model.py
"""Subprocess dependency model."""

import uuid
from typing import Any, ClassVar, Dict, List, Sequence

from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord


class SubProcessDependency(UUIDMixin, TimestampMixin, ActiveRecord):
    """Dependency edge from a subprocess to one upstream subprocess."""

    __table_name__ = "stateflow_subprocess_dependencies"

    process_id: uuid.UUID
    subprocess_id: uuid.UUID
    depends_on_id: uuid.UUID

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def for_subprocess(
        cls,
        process_id: uuid.UUID,
        subprocess: Any,
        depends_on: Any,
    ) -> "SubProcessDependency":
        """Build a dependency edge for a subprocess."""
        return cls(
            process_id=process_id,
            subprocess_id=subprocess.id,
            depends_on_id=depends_on.id,
        )

    @classmethod
    def group_by_subprocess(
        cls,
        dependencies: Sequence["SubProcessDependency"],
    ) -> Dict[object, List["SubProcessDependency"]]:
        """Group dependency edges by downstream subprocess id."""
        dependencies_by_subprocess: Dict[object, List[SubProcessDependency]] = {}
        for dependency in dependencies:
            dependencies_by_subprocess.setdefault(dependency.subprocess_id, []).append(dependency)
        return dependencies_by_subprocess


class AsyncSubProcessDependency(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    """Async sibling of :class:`SubProcessDependency`."""

    __table_name__ = "stateflow_subprocess_dependencies"

    process_id: uuid.UUID
    subprocess_id: uuid.UUID
    depends_on_id: uuid.UUID

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def for_subprocess(
        cls,
        process_id: uuid.UUID,
        subprocess: Any,
        depends_on: Any,
    ) -> "AsyncSubProcessDependency":
        """Build a dependency edge for a subprocess."""
        return cls(
            process_id=process_id,
            subprocess_id=subprocess.id,
            depends_on_id=depends_on.id,
        )

    @classmethod
    def group_by_subprocess(
        cls,
        dependencies: Sequence["AsyncSubProcessDependency"],
    ) -> Dict[object, List["AsyncSubProcessDependency"]]:
        """Group dependency edges by downstream subprocess id."""
        dependencies_by_subprocess: Dict[object, List[AsyncSubProcessDependency]] = {}
        for dependency in dependencies:
            dependencies_by_subprocess.setdefault(dependency.subprocess_id, []).append(dependency)
        return dependencies_by_subprocess
