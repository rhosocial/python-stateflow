# src/rhosocial/stateflow/models/order_template/model.py
"""Order template model."""

from datetime import datetime
from typing import ClassVar, Dict, Optional, Sequence, Set, TypeVar

from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from ...exceptions import TemplateValidationError
from ...types import TEMPLATE_STATUS_DRAFT
from ..order_template_step import OrderTemplateStep

StepT = TypeVar("StepT", bound=OrderTemplateStep)


class OrderTemplate(UUIDMixin, TimestampMixin, ActiveRecord):
    """Process blueprint that should evolve through new versions after publication."""

    __table_name__ = "stateflow_order_templates"

    name: str
    version: int = 1
    status: str = TEMPLATE_STATUS_DRAFT
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    deprecated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    checksum: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def ordered_steps(self, steps: Sequence[StepT]) -> Sequence[StepT]:
        """Return template steps ordered by their configured sequence."""
        return sorted(steps, key=lambda step: step.step_order)

    def steps_before(self, steps: Sequence[OrderTemplateStep], start_from: str) -> Set[str]:
        """Return step names that appear before the selected entry point."""
        before: Set[str] = set()
        for step in steps:
            if step.name == start_from:
                return before
            before.add(step.name)
        raise TemplateValidationError(f"Unknown start_from step: {start_from}")

    def snapshot(self, steps: Sequence[OrderTemplateStep]) -> Dict:
        """Build an immutable template snapshot for a runtime process."""
        return {
            "template": {
                "id": str(self.id),
                "name": self.name,
                "version": self.version,
                "status": self.status,
            },
            "steps": [
                {
                    "name": step.name,
                    "handler_class": step.handler_class,
                    "terminal_states": list(step.terminal_states),
                    "advance_states": list(step.advance_states),
                    "rollback_states": list(step.rollback_states),
                    "timeout_seconds": step.timeout_seconds,
                    "timeout_status": step.timeout_status,
                    "depends_on": list(step.depends_on),
                    "step_order": step.step_order,
                }
                for step in steps
            ],
        }


class AsyncOrderTemplate(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    """Async sibling of :class:`OrderTemplate`."""

    __table_name__ = "stateflow_order_templates"

    name: str
    version: int = 1
    status: str = TEMPLATE_STATUS_DRAFT
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    deprecated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    checksum: Optional[str] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    def ordered_steps(self, steps: Sequence[StepT]) -> Sequence[StepT]:
        """Return template steps ordered by their configured sequence."""
        return sorted(steps, key=lambda step: step.step_order)

    def steps_before(self, steps: Sequence[OrderTemplateStep], start_from: str) -> Set[str]:
        """Return step names that appear before the selected entry point."""
        before: Set[str] = set()
        for step in steps:
            if step.name == start_from:
                return before
            before.add(step.name)
        raise TemplateValidationError(f"Unknown start_from step: {start_from}")

    def snapshot(self, steps: Sequence[OrderTemplateStep]) -> Dict:
        """Build an immutable template snapshot for a runtime process."""
        return {
            "template": {
                "id": str(self.id),
                "name": self.name,
                "version": self.version,
                "status": self.status,
            },
            "steps": [
                {
                    "name": step.name,
                    "handler_class": step.handler_class,
                    "terminal_states": list(step.terminal_states),
                    "advance_states": list(step.advance_states),
                    "rollback_states": list(step.rollback_states),
                    "timeout_seconds": step.timeout_seconds,
                    "timeout_status": step.timeout_status,
                    "depends_on": list(step.depends_on),
                    "step_order": step.step_order,
                }
                for step in steps
            ],
        }
