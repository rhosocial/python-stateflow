# src/rhosocial/stateflow/dispatcher.py
"""Event dispatchers for stateflow."""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from .exceptions import DuplicateEventError, InvalidStateTransitionError
from .models import Order, OrderEvent, OrderOutbox, OrderSubProcess, SubProcessDependency
from .types import (
    EVENT_CONFLICT,
    EVENT_ORDER_COMPLETED,
    EVENT_SP_STARTED,
    EVENT_SP_STATUS_CHANGED,
    EVENT_SP_TIMEOUT,
    ORDER_STATUS_COMPLETED,
    OUTBOX_TOPIC_HANDLER_START,
    SUBPROCESS_STATUS_RUNNING,
)


class DispatchResult:
    """Result object returned by a dispatcher event handling cycle."""

    def __init__(
        self,
        event: OrderEvent,
        started_subprocesses: Optional[List[OrderSubProcess]] = None,
        outbox_items: Optional[List[OrderOutbox]] = None,
        duplicate: bool = False,
    ):
        self.event = event
        self.started_subprocesses = started_subprocesses or []
        self.outbox_items = outbox_items or []
        self.duplicate = duplicate


class SyncOrderDispatcher:
    """Stateless synchronous dispatcher for subprocess events."""

    def on_event(
        self,
        order: Order,
        subprocess: OrderSubProcess,
        *,
        new_status: str,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
        events: Optional[Sequence[OrderEvent]] = None,
        event_key: Optional[str] = None,
        payload: Optional[Dict] = None,
    ) -> DispatchResult:
        """Apply one event, enforce idempotency, and enqueue downstream side effects."""
        events = list(events or [])
        if event_key:
            existing = self._find_event_by_key(events, event_key)
            if existing:
                return DispatchResult(existing, duplicate=True)

        if subprocess.skipped:
            raise InvalidStateTransitionError("Skipped subprocess cannot receive events")

        if self._is_terminal(subprocess.status, subprocess):
            if subprocess.status == new_status:
                event = OrderEvent(
                    order_id=order.id,
                    subprocess_id=subprocess.id,
                    event_type=EVENT_SP_STATUS_CHANGED,
                    from_status=subprocess.status,
                    to_status=new_status,
                    payload=payload or {},
                    event_key=event_key,
                )
                return DispatchResult(event)
            conflict_event = OrderEvent(
                order_id=order.id,
                subprocess_id=subprocess.id,
                event_type=EVENT_CONFLICT,
                from_status=subprocess.status,
                to_status=new_status,
                payload=payload or {},
                event_key=event_key,
                conflict=True,
            )
            return DispatchResult(conflict_event)

        previous_status = subprocess.status
        subprocess.status = new_status
        if self._is_terminal(new_status, subprocess):
            subprocess.completed_at = datetime.now(timezone.utc)

        event = OrderEvent(
            order_id=order.id,
            subprocess_id=subprocess.id,
            event_type=EVENT_SP_STATUS_CHANGED,
            from_status=previous_status,
            to_status=new_status,
            payload=payload or {},
            event_key=event_key,
        )

        started = []
        outbox_items = []
        if new_status in subprocess.advance_states:
            started = self._start_ready_subprocesses(subprocesses, dependencies)
            outbox_items = [
                OrderOutbox(
                    event_id=event.id,
                    topic=OUTBOX_TOPIC_HANDLER_START,
                    payload={"subprocess_id": str(started_subprocess.id)},
                )
                for started_subprocess in started
            ]

        if self._all_completed(subprocesses):
            order.status = ORDER_STATUS_COMPLETED
            order.completed_at = datetime.now(timezone.utc)
            events.append(
                OrderEvent(
                    order_id=order.id,
                    event_type=EVENT_ORDER_COMPLETED,
                    to_status=ORDER_STATUS_COMPLETED,
                )
            )

        return DispatchResult(event, started_subprocesses=started, outbox_items=outbox_items)

    def on_timeout(
        self,
        order: Order,
        subprocess: OrderSubProcess,
        *,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
        events: Optional[Sequence[OrderEvent]] = None,
    ) -> DispatchResult:
        """Handle a subprocess timeout."""
        if not subprocess.timeout_status:
            raise InvalidStateTransitionError("Subprocess has no timeout_status")
        return self.on_event(
            order,
            subprocess,
            new_status=subprocess.timeout_status,
            subprocesses=subprocesses,
            dependencies=dependencies,
            events=events,
            payload={"event_type": EVENT_SP_TIMEOUT},
        )

    def _find_event_by_key(
        self,
        events: Sequence[OrderEvent],
        event_key: str,
    ) -> Optional[OrderEvent]:
        for event in events:
            if event.event_key == event_key:
                return event
        return None

    def _is_terminal(self, status: str, subprocess: OrderSubProcess) -> bool:
        return status in subprocess.terminal_states

    def _start_ready_subprocesses(
        self,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
    ) -> List[OrderSubProcess]:
        subprocess_by_id = {subprocess.id: subprocess for subprocess in subprocesses}
        dependencies_by_subprocess: Dict[object, List[SubProcessDependency]] = {}
        for dependency in dependencies:
            dependencies_by_subprocess.setdefault(dependency.subprocess_id, []).append(dependency)

        started = []
        for candidate in subprocesses:
            if candidate.skipped or candidate.status != "pending":
                continue
            candidate_dependencies = dependencies_by_subprocess.get(candidate.id, [])
            if not candidate_dependencies:
                continue
            if all(
                self._dependency_satisfied(subprocess_by_id[dependency.depends_on_id])
                for dependency in candidate_dependencies
            ):
                candidate.status = SUBPROCESS_STATUS_RUNNING
                candidate.started_at = datetime.now(timezone.utc)
                started.append(candidate)
        return started

    def _dependency_satisfied(self, subprocess: OrderSubProcess) -> bool:
        return subprocess.skipped or subprocess.status in subprocess.advance_states

    def _all_completed(self, subprocesses: Sequence[OrderSubProcess]) -> bool:
        active_subprocesses = [subprocess for subprocess in subprocesses if not subprocess.skipped]
        return bool(active_subprocesses) and all(
            subprocess.status in subprocess.advance_states for subprocess in active_subprocesses
        )


class AsyncOrderDispatcher:
    """Async dispatcher facade preserving method parity with SyncOrderDispatcher."""

    def __init__(self):
        self._sync_dispatcher = SyncOrderDispatcher()

    async def on_event(self, *args, **kwargs) -> DispatchResult:
        """Handle a subprocess event asynchronously."""
        return self._sync_dispatcher.on_event(*args, **kwargs)

    async def on_timeout(self, *args, **kwargs) -> DispatchResult:
        """Handle a subprocess timeout asynchronously."""
        return self._sync_dispatcher.on_timeout(*args, **kwargs)
