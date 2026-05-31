# src/rhosocial/stateflow/models/order_template_step/model.py
"""Order template step model."""

import uuid
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord


class OrderTemplateStep(UUIDMixin, TimestampMixin, ActiveRecord):
    """Subprocess definition with dependency and state classification rules."""

    __table_name__ = "stateflow_order_template_steps"

    template_id: uuid.UUID
    name: str
    handler_class: str
    terminal_states: List[str] = Field(default_factory=list)
    advance_states: List[str] = Field(default_factory=list)
    rollback_states: List[str] = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    timeout_status: Optional[str] = None
    on_start_notify: Optional[Dict[str, Any]] = None
    on_complete_notify: Optional[Dict[str, Any]] = None
    on_rollback_notify: Optional[Dict[str, Any]] = None
    on_timeout_notify: Optional[Dict[str, Any]] = None
    depends_on: List[str] = Field(default_factory=list)
    step_order: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()

    def terminal_state_set(self) -> Set[str]:
        """Return terminal states as a set for validation and state checks."""
        return set(self.terminal_states)

    def advance_state_set(self) -> Set[str]:
        """Return advance states as a set for validation and completion checks."""
        return set(self.advance_states)

    def rollback_state_set(self) -> Set[str]:
        """Return rollback states as a set for validation."""
        return set(self.rollback_states)

    def missing_advance_states(self) -> Set[str]:
        """Return advance states that are not declared as terminal states."""
        return self.advance_state_set() - self.terminal_state_set()

    def missing_rollback_states(self) -> Set[str]:
        """Return rollback states that are not declared as terminal states."""
        return self.rollback_state_set() - self.terminal_state_set()

    def requires_timeout_status(self) -> bool:
        """Return whether timeout_seconds requires a timeout status."""
        return self.timeout_seconds is not None and not self.timeout_status

    def has_terminal_timeout_status(self) -> bool:
        """Return whether the configured timeout status is terminal or absent."""
        return not self.timeout_status or self.timeout_status in self.terminal_state_set()
