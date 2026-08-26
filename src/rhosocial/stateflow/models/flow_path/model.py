# src/rhosocial/stateflow/models/flow_path/model.py
"""Flow path model."""

import uuid
from typing import ClassVar, List, Optional, Set

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord


class FlowPath(UUIDMixin, TimestampMixin, ActiveRecord):
    """Path variant declaring an entry step and skipped steps."""

    __table_name__ = "stateflow_flow_paths"

    template_id: uuid.UUID
    name: str
    skip_steps: list = Field(default_factory=list)
    start_from: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def has_unknown_start_from(self, step_names: Set[str]) -> bool:
        """Return whether start_from references a missing step."""
        return bool(self.start_from and self.start_from not in step_names)

    def unknown_skip_steps(self, step_names: Set[str]) -> List[str]:
        """Return skipped steps that do not exist in the template."""
        return [step_name for step_name in self.skip_steps if step_name not in step_names]


class AsyncFlowPath(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    """Async sibling of :class:`FlowPath`."""

    __table_name__ = "stateflow_flow_paths"

    template_id: uuid.UUID
    name: str
    skip_steps: list = Field(default_factory=list)
    start_from: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def has_unknown_start_from(self, step_names: Set[str]) -> bool:
        """Return whether start_from references a missing step."""
        return bool(self.start_from and self.start_from not in step_names)

    def unknown_skip_steps(self, step_names: Set[str]) -> List[str]:
        """Return skipped steps that do not exist in the template."""
        return [step_name for step_name in self.skip_steps if step_name not in step_names]
