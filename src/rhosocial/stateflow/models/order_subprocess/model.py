# src/rhosocial/stateflow/models/order_subprocess/model.py
"""Order subprocess model."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, Dict, Optional, Sequence, Set

from pydantic import Field
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import OptimisticLockMixin, TimestampMixin, UUIDMixin
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from ..order_process import OrderProcess
from ..order_template_step import OrderTemplateStep
from ...types import (
    ROLLBACK_STATUS_COMPLETED,
    ROLLBACK_STATUS_FAILED,
    ROLLBACK_STATUS_NOT_REQUIRED,
    ROLLBACK_STATUS_RUNNING,
    SUBPROCESS_SOURCE_DYNAMIC,
    SUBPROCESS_SOURCE_TEMPLATE,
    SUBPROCESS_STATUS_PENDING,
    SUBPROCESS_STATUS_RUNNING,
)


class OrderSubProcess(UUIDMixin, TimestampMixin, OptimisticLockMixin, ActiveRecord):
    """Runtime state for one subprocess with optimistic concurrency control."""

    __table_name__ = "stateflow_order_subprocesses"

    process_id: uuid.UUID
    step_name: str
    status: str = SUBPROCESS_STATUS_PENDING
    handler_class: str
    terminal_states: list = Field(default_factory=list)
    advance_states: list = Field(default_factory=list)
    rollback_states: list = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    timeout_status: Optional[str] = None
    started_at: Optional[datetime] = None
    timeout_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    skipped: bool = False
    extra: dict = Field(default_factory=dict)
    source: str = SUBPROCESS_SOURCE_TEMPLATE
    sequence: int = 0
    created_event_id: Optional[uuid.UUID] = None
    is_reversible: bool = False
    rollback_status: str = ROLLBACK_STATUS_NOT_REQUIRED
    rollback_started_at: Optional[datetime] = None
    rollback_completed_at: Optional[datetime] = None
    rollback_error: Optional[dict] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def from_template_step(
        cls,
        process_id: uuid.UUID,
        step: OrderTemplateStep,
        skipped: bool,
        sequence: int,
    ) -> "OrderSubProcess":
        """Build a runtime subprocess from a template step."""
        return cls(
            process_id=process_id,
            step_name=step.name,
            status=SUBPROCESS_STATUS_PENDING,
            handler_class=step.handler_class,
            terminal_states=list(step.terminal_states),
            advance_states=list(step.advance_states),
            rollback_states=list(step.rollback_states),
            timeout_seconds=step.timeout_seconds,
            timeout_status=step.timeout_status,
            skipped=skipped,
            source=SUBPROCESS_SOURCE_TEMPLATE,
            sequence=sequence,
        )

    @classmethod
    def dynamic(
        cls,
        process: OrderProcess,
        existing_subprocesses: Sequence["OrderSubProcess"],
        *,
        name: str,
        handler_class: str,
        terminal_states: Sequence[str],
        advance_states: Sequence[str],
        rollback_states: Optional[Sequence[str]] = None,
        timeout_seconds: Optional[int] = None,
        timeout_status: Optional[str] = None,
        is_reversible: bool = False,
    ) -> "OrderSubProcess":
        """Build a dynamic subprocess appended to an existing process."""
        sequence = max((subprocess.sequence for subprocess in existing_subprocesses), default=-1) + 1
        return cls(
            process_id=process.id,
            step_name=name,
            status=SUBPROCESS_STATUS_PENDING,
            handler_class=handler_class,
            terminal_states=list(terminal_states),
            advance_states=list(advance_states),
            rollback_states=list(rollback_states or []),
            timeout_seconds=timeout_seconds,
            timeout_status=timeout_status,
            source=SUBPROCESS_SOURCE_DYNAMIC,
            sequence=sequence,
            is_reversible=is_reversible,
        )

    def is_terminal(self, status: Optional[str] = None) -> bool:
        """Return whether a status is terminal for this subprocess."""
        return (self.status if status is None else status) in self.terminal_states

    def is_advance_status(self, status: Optional[str] = None) -> bool:
        """Return whether a status advances the process."""
        return (self.status if status is None else status) in self.advance_states

    def can_receive_event(self) -> bool:
        """Return whether this subprocess can receive external events."""
        return not self.skipped

    def apply_status(self, new_status: str) -> str:
        """Apply a new status and return the previous status."""
        previous_status = self.status
        self.status = new_status
        if self.is_terminal(new_status):
            self.completed_at = datetime.now(timezone.utc)
        return previous_status

    def mark_running(self) -> None:
        """Mark the subprocess as running and schedule its timeout if any."""
        now = datetime.now(timezone.utc)
        self.status = SUBPROCESS_STATUS_RUNNING
        self.started_at = now
        if self.timeout_seconds is not None:
            self.timeout_at = now + timedelta(seconds=self.timeout_seconds)

    def dependency_satisfied(self) -> bool:
        """Return whether this subprocess satisfies downstream dependencies."""
        return self.skipped or self.is_advance_status()

    def rollback_state_set(self) -> Set[str]:
        """Return rollback states as a set."""
        return set(self.rollback_states)

    def can_rollback(self) -> bool:
        """Return whether this subprocess may begin a rollback now."""
        return (
            self.is_reversible
            and self.rollback_status == ROLLBACK_STATUS_NOT_REQUIRED
            and not self.skipped
            and self.status in self.rollback_state_set()
        )

    def begin_rollback(self) -> None:
        """Mark the rollback as in-progress at the current UTC time."""
        self.rollback_status = ROLLBACK_STATUS_RUNNING
        self.rollback_started_at = datetime.now(timezone.utc)

    def complete_rollback(self) -> None:
        """Mark the rollback as completed at the current UTC time."""
        self.rollback_status = ROLLBACK_STATUS_COMPLETED
        self.rollback_completed_at = datetime.now(timezone.utc)

    def fail_rollback(self, error: Dict[str, Any]) -> None:
        """Mark the rollback as failed and record the error payload."""
        self.rollback_status = ROLLBACK_STATUS_FAILED
        self.rollback_error = error
        self.rollback_completed_at = datetime.now(timezone.utc)


class AsyncOrderSubProcess(UUIDMixin, TimestampMixin, OptimisticLockMixin, AsyncActiveRecord):
    """Async sibling of :class:`OrderSubProcess`."""

    __table_name__ = "stateflow_order_subprocesses"

    process_id: uuid.UUID
    step_name: str
    status: str = SUBPROCESS_STATUS_PENDING
    handler_class: str
    terminal_states: list = Field(default_factory=list)
    advance_states: list = Field(default_factory=list)
    rollback_states: list = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    timeout_status: Optional[str] = None
    started_at: Optional[datetime] = None
    timeout_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    skipped: bool = False
    extra: dict = Field(default_factory=dict)
    source: str = SUBPROCESS_SOURCE_TEMPLATE
    sequence: int = 0
    created_event_id: Optional[uuid.UUID] = None
    is_reversible: bool = False
    rollback_status: str = ROLLBACK_STATUS_NOT_REQUIRED
    rollback_started_at: Optional[datetime] = None
    rollback_completed_at: Optional[datetime] = None
    rollback_error: Optional[dict] = None

    c: ClassVar[FieldProxy] = FieldProxy()

    @classmethod
    def from_template_step(
        cls,
        process_id: uuid.UUID,
        step: OrderTemplateStep,
        skipped: bool,
        sequence: int,
    ) -> "AsyncOrderSubProcess":
        """Build a runtime subprocess from a template step."""
        return cls(
            process_id=process_id,
            step_name=step.name,
            status=SUBPROCESS_STATUS_PENDING,
            handler_class=step.handler_class,
            terminal_states=list(step.terminal_states),
            advance_states=list(step.advance_states),
            rollback_states=list(step.rollback_states),
            timeout_seconds=step.timeout_seconds,
            timeout_status=step.timeout_status,
            skipped=skipped,
            source=SUBPROCESS_SOURCE_TEMPLATE,
            sequence=sequence,
        )

    @classmethod
    def dynamic(
        cls,
        process: OrderProcess,
        existing_subprocesses: Sequence["AsyncOrderSubProcess"],
        *,
        name: str,
        handler_class: str,
        terminal_states: Sequence[str],
        advance_states: Sequence[str],
        rollback_states: Optional[Sequence[str]] = None,
        timeout_seconds: Optional[int] = None,
        timeout_status: Optional[str] = None,
        is_reversible: bool = False,
    ) -> "AsyncOrderSubProcess":
        """Build a dynamic subprocess appended to an existing process."""
        sequence = max((subprocess.sequence for subprocess in existing_subprocesses), default=-1) + 1
        return cls(
            process_id=process.id,
            step_name=name,
            status=SUBPROCESS_STATUS_PENDING,
            handler_class=handler_class,
            terminal_states=list(terminal_states),
            advance_states=list(advance_states),
            rollback_states=list(rollback_states or []),
            timeout_seconds=timeout_seconds,
            timeout_status=timeout_status,
            source=SUBPROCESS_SOURCE_DYNAMIC,
            sequence=sequence,
            is_reversible=is_reversible,
        )

    def is_terminal(self, status: Optional[str] = None) -> bool:
        """Return whether a status is terminal for this subprocess."""
        return (self.status if status is None else status) in self.terminal_states

    def is_advance_status(self, status: Optional[str] = None) -> bool:
        """Return whether a status advances the process."""
        return (self.status if status is None else status) in self.advance_states

    def can_receive_event(self) -> bool:
        """Return whether this subprocess can receive external events."""
        return not self.skipped

    def apply_status(self, new_status: str) -> str:
        """Apply a new status and return the previous status."""
        previous_status = self.status
        self.status = new_status
        if self.is_terminal(new_status):
            self.completed_at = datetime.now(timezone.utc)
        return previous_status

    def mark_running(self) -> None:
        """Mark the subprocess as running and schedule its timeout if any."""
        now = datetime.now(timezone.utc)
        self.status = SUBPROCESS_STATUS_RUNNING
        self.started_at = now
        if self.timeout_seconds is not None:
            self.timeout_at = now + timedelta(seconds=self.timeout_seconds)

    def dependency_satisfied(self) -> bool:
        """Return whether this subprocess satisfies downstream dependencies."""
        return self.skipped or self.is_advance_status()

    def rollback_state_set(self) -> Set[str]:
        """Return rollback states as a set."""
        return set(self.rollback_states)

    def can_rollback(self) -> bool:
        """Return whether this subprocess may begin a rollback now."""
        return (
            self.is_reversible
            and self.rollback_status == ROLLBACK_STATUS_NOT_REQUIRED
            and not self.skipped
            and self.status in self.rollback_state_set()
        )

    def begin_rollback(self) -> None:
        """Mark the rollback as in-progress at the current UTC time."""
        self.rollback_status = ROLLBACK_STATUS_RUNNING
        self.rollback_started_at = datetime.now(timezone.utc)

    def complete_rollback(self) -> None:
        """Mark the rollback as completed at the current UTC time."""
        self.rollback_status = ROLLBACK_STATUS_COMPLETED
        self.rollback_completed_at = datetime.now(timezone.utc)

    def fail_rollback(self, error: Dict[str, Any]) -> None:
        """Mark the rollback as failed and record the error payload."""
        self.rollback_status = ROLLBACK_STATUS_FAILED
        self.rollback_error = error
        self.rollback_completed_at = datetime.now(timezone.utc)
