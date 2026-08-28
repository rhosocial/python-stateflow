# src/rhosocial/stateflow/dispatcher.py
"""Event dispatchers for stateflow.

The dispatcher is a **stateless** state machine: it advances in-memory model
instances and yields new events/outbox items, performing no I/O. Sync and
async dispatchers share the same pure logic via :class:`_DispatcherBase`;
they differ only in which model classes the factory methods target
(``OrderEvent`` vs ``AsyncOrderEvent``, ``OrderOutbox`` vs ``AsyncOrderOutbox``)
so that the produced objects match the caller's sync/async path.
"""

from typing import Any, ClassVar, Dict, List, Optional, Sequence

from .exceptions import InvalidStateTransitionError
from .models import (
    AsyncOrderEvent,
    AsyncOrderOutbox,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderSubProcess,
    SubProcessDependency,
)
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


class _DispatcherBase:
    """Shared stateless dispatch logic.

    Subclasses set ``_event_cls`` and ``_outbox_cls`` to the appropriate
    sync or async model classes so that factory methods produce the right
    concrete types.
    """

    _event_cls: ClassVar[Any]
    _outbox_cls: ClassVar[Any]

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
        existing = self._event_cls.find_by_event_key(events, event_key)
        if existing:
            return DispatchResult(existing, duplicate=True)

        if not subprocess.can_receive_event():
            raise InvalidStateTransitionError("Skipped subprocess cannot receive events")

        if subprocess.is_terminal():
            if subprocess.status == new_status:
                event = self._event_cls.status_changed(
                    order, subprocess, subprocess.status, new_status, payload, event_key,
                )
                return DispatchResult(event)
            conflict_event = self._event_cls.conflict_event(
                order, subprocess, new_status, payload, event_key,
            )
            return DispatchResult(conflict_event)

        previous_status = subprocess.apply_status(new_status)
        event = self._event_cls.status_changed(
            order, subprocess, previous_status, new_status, payload, event_key,
        )

        started = []
        outbox_items = []
        if subprocess.is_advance_status(new_status):
            started = self._start_ready_subprocesses(subprocesses, dependencies)
            outbox_items = [self._outbox_cls.handler_start(event, sp) for sp in started]

        if order.all_subprocesses_completed(subprocesses):
            order.mark_completed()
            events.append(self._event_cls.order_completed(order))

        return DispatchResult(event, started_subprocesses=started, outbox_items=outbox_items)

    def on_timeout(
        self,
        order: Order,
        subprocess: OrderSubProcess,
        *,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
        events: Optional[Sequence[OrderEvent]] = None,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Handle a subprocess timeout.

        ``event_key`` makes the timeout idempotent — a repeated call with the
        same key returns ``duplicate=True`` instead of re-applying the
        transition, so failed timeouts can be safely retried by the sweeper.
        """
        if not subprocess.timeout_status:
            raise InvalidStateTransitionError("Subprocess has no timeout_status")
        return self.on_event(
            order,
            subprocess,
            new_status=subprocess.timeout_status,
            subprocesses=subprocesses,
            dependencies=dependencies,
            events=events,
            event_key=event_key,
            payload={"event_type": EVENT_SP_TIMEOUT},
        )

    def on_rollback(
        self,
        order: Order,
        subprocess: OrderSubProcess,
        *,
        events: Optional[Sequence[OrderEvent]] = None,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Begin a rollback for a reversible subprocess in a rollback state."""
        events = list(events or [])
        existing = self._event_cls.find_by_event_key(events, event_key)
        if existing:
            return DispatchResult(existing, duplicate=True)

        if not subprocess.can_rollback():
            raise InvalidStateTransitionError(
                f"Subprocess '{subprocess.step_name}' cannot be rolled back "
                f"(reversible={subprocess.is_reversible}, "
                f"rollback_status={subprocess.rollback_status}, "
                f"status={subprocess.status})"
            )

        subprocess.begin_rollback()
        event = self._event_cls.rollback_started(order, subprocess, event_key)
        outbox_items = [self._outbox_cls.handler_rollback(event, subprocess)]
        return DispatchResult(event, outbox_items=outbox_items)

    @staticmethod
    def _group_by_subprocess(dependencies):
        """Group dependency edges by downstream subprocess id (pure logic)."""
        grouped: Dict = {}
        for dependency in dependencies:
            grouped.setdefault(dependency.subprocess_id, []).append(dependency)
        return grouped

    def _start_ready_subprocesses(
        self,
        subprocesses: Sequence[OrderSubProcess],
        dependencies: Sequence[SubProcessDependency],
    ) -> List[OrderSubProcess]:
        subprocess_by_id = {sp.id: sp for sp in subprocesses}
        dependencies_by_subprocess = self._group_by_subprocess(dependencies)

        started = []
        for candidate in subprocesses:
            if candidate.skipped or candidate.status != SUBPROCESS_STATUS_PENDING:
                continue
            candidate_dependencies = dependencies_by_subprocess.get(candidate.id, [])
            if not candidate_dependencies:
                continue
            if all(
                subprocess_by_id[dep.depends_on_id].dependency_satisfied()
                for dep in candidate_dependencies
            ):
                candidate.mark_running()
                started.append(candidate)
        return started


class SyncOrderDispatcher(_DispatcherBase):
    """Synchronous dispatcher producing sync model instances."""

    _event_cls = OrderEvent
    _outbox_cls = OrderOutbox


class AsyncOrderDispatcher(_DispatcherBase):
    """Asynchronous dispatcher producing async model instances.

    The dispatch logic itself is pure (no I/O) so the ``async def`` methods
    exist for API parity with :class:`SyncOrderDispatcher`. The async
    distinction matters at the service layer where DB I/O occurs.

    Note: ``on_timeout`` and ``on_rollback`` are overridden because the base
    implementations call ``self.on_event(...)`` which is ``async def`` here
    and returns a coroutine — the async overrides must ``await`` it.
    """

    _event_cls = AsyncOrderEvent
    _outbox_cls = AsyncOrderOutbox

    async def on_event(self, *args, **kwargs) -> DispatchResult:  # type: ignore[override]
        """Handle a subprocess event asynchronously."""
        return super().on_event(*args, **kwargs)

    async def on_timeout(  # type: ignore[override]
        self,
        order,
        subprocess,
        *,
        subprocesses,
        dependencies,
        events=None,
        event_key=None,
    ) -> DispatchResult:
        """Handle a subprocess timeout asynchronously."""
        if not subprocess.timeout_status:
            raise InvalidStateTransitionError("Subprocess has no timeout_status")
        return await self.on_event(
            order,
            subprocess,
            new_status=subprocess.timeout_status,
            subprocesses=subprocesses,
            dependencies=dependencies,
            events=events,
            event_key=event_key,
            payload={"event_type": EVENT_SP_TIMEOUT},
        )

    async def on_rollback(self, *args, **kwargs) -> DispatchResult:  # type: ignore[override]
        """Begin a subprocess rollback asynchronously.

        The base ``on_rollback`` does not call ``self.on_event`` so it is safe
        to delegate directly.
        """
        return super().on_rollback(*args, **kwargs)
