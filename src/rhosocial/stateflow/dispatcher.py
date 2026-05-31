# src/rhosocial/stateflow/dispatcher.py
"""Event dispatchers for stateflow."""

from typing import Dict, List, Optional, Sequence

from .exceptions import InvalidStateTransitionError
from .models import Order, OrderEvent, OrderOutbox, OrderSubProcess, SubProcessDependency
from .types import EVENT_SP_TIMEOUT, SUBPROCESS_STATUS_PENDING


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
        existing = OrderEvent.find_by_event_key(events, event_key)
        if existing:
            return DispatchResult(existing, duplicate=True)

        if not subprocess.can_receive_event():
            raise InvalidStateTransitionError("Skipped subprocess cannot receive events")

        if subprocess.is_terminal():
            if subprocess.status == new_status:
                event = OrderEvent.status_changed(
                    order,
                    subprocess,
                    subprocess.status,
                    new_status,
                    payload,
                    event_key,
                )
                return DispatchResult(event)
            conflict_event = OrderEvent.conflict_event(
                order,
                subprocess,
                new_status,
                payload,
                event_key,
            )
            return DispatchResult(conflict_event)

        previous_status = subprocess.apply_status(new_status)
        event = OrderEvent.status_changed(
            order,
            subprocess,
            previous_status,
            new_status,
            payload,
            event_key,
        )

        started = []
        outbox_items = []
        if subprocess.is_advance_status(new_status):
            started = self._start_ready_subprocesses(subprocesses, dependencies)
            outbox_items = [
                OrderOutbox.handler_start(event, started_subprocess)
                for started_subprocess in started
            ]

        if order.all_subprocesses_completed(subprocesses):
            order.mark_completed()
            events.append(OrderEvent.order_completed(order))

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

    def _start_ready_subprocesses(
        self,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
    ) -> List[OrderSubProcess]:
        subprocess_by_id = {subprocess.id: subprocess for subprocess in subprocesses}
        dependencies_by_subprocess = SubProcessDependency.group_by_subprocess(dependencies)

        started = []
        for candidate in subprocesses:
            if candidate.skipped or candidate.status != SUBPROCESS_STATUS_PENDING:
                continue
            candidate_dependencies = dependencies_by_subprocess.get(candidate.id, [])
            if not candidate_dependencies:
                continue
            if all(
                subprocess_by_id[dependency.depends_on_id].dependency_satisfied()
                for dependency in candidate_dependencies
            ):
                candidate.mark_running()
                started.append(candidate)
        return started


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
