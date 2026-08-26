# src/rhosocial/stateflow/service.py
"""Transactional service layer binding the stateless dispatcher to ActiveRecord persistence.

Sync and async services are **fully parallel and non-interoperable**: the sync
service uses sync models (``Order``, ``OrderSubProcess``, …) with synchronous
``save()`` / ``query().all()`` / ``backend.transaction()``; the async service
uses async models (``AsyncOrder``, ``AsyncOrderSubProcess``, …) with native
``await`` at every DB call site. No ``asyncio.to_thread`` bridging is used.

Concurrency model
-----------------
``OrderSubProcess`` carries :class:`~rhosocial.activerecord.field.OptimisticLockMixin`,
so a ``save()`` whose version no longer matches raises
:class:`~rhosocial.activerecord.backend.errors.DatabaseError`. Both services
catch that case and re-raise as
:class:`~rhosocial.stateflow.exceptions.ConcurrentStateTransitionError`.
"""

from typing import Any, Dict, Optional

from .dispatcher import AsyncOrderDispatcher, DispatchResult, SyncOrderDispatcher
from .exceptions import ConcurrentStateTransitionError
from .models import (
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderSubProcess,
    AsyncSubProcessDependency,
    Order,
    OrderEvent,
    OrderSubProcess,
    SubProcessDependency,
)

_CONCURRENCY_MESSAGE = "Record was updated by another process"


class SyncOrderService:
    """Synchronous service that loads, advances, and persists an order in one transaction."""

    def __init__(self, dispatcher: Optional[SyncOrderDispatcher] = None) -> None:
        self.dispatcher = dispatcher or SyncOrderDispatcher()

    def publish_event(
        self,
        order_id: Any,
        subprocess_id: Any,
        *,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Atomically load → dispatch → persist a subprocess status transition."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = Order.backend()
        with backend.transaction():
            order = Order.query().where(Order.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            seed = (
                OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess_id).one()
            )
            if seed is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            process_id = seed.process_id
            subprocesses = (
                OrderSubProcess.query()
                .where(OrderSubProcess.c.process_id == process_id)
                .all()
            )
            subprocess = next(
                (sp for sp in subprocesses if sp.id == subprocess_id), None
            )
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found in process {process_id}")

            dependencies = (
                SubProcessDependency.query()
                .where(SubProcessDependency.c.process_id == process_id)
                .all()
            )
            existing_events = (
                OrderEvent.query().where(OrderEvent.c.order_id == order_id).all()
            )

            result = self.dispatcher.on_event(
                order=order,
                subprocess=subprocess,
                new_status=new_status,
                subprocesses=subprocesses,
                dependencies=dependencies,
                events=existing_events,
                event_key=event_key,
                payload=payload,
            )

            if result.duplicate:
                return result

            try:
                self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    def publish_timeout(
        self,
        order_id: Any,
        subprocess_id: Any,
    ) -> DispatchResult:
        """Atomically apply a subprocess timeout transition."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = Order.backend()
        with backend.transaction():
            order = Order.query().where(Order.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            seed = (
                OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess_id).one()
            )
            if seed is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            process_id = seed.process_id
            subprocesses = (
                OrderSubProcess.query()
                .where(OrderSubProcess.c.process_id == process_id)
                .all()
            )
            subprocess = next(
                (sp for sp in subprocesses if sp.id == subprocess_id), None
            )
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found in process {process_id}")

            dependencies = (
                SubProcessDependency.query()
                .where(SubProcessDependency.c.process_id == process_id)
                .all()
            )
            existing_events = (
                OrderEvent.query().where(OrderEvent.c.order_id == order_id).all()
            )

            result = self.dispatcher.on_timeout(
                order=order,
                subprocess=subprocess,
                subprocesses=subprocesses,
                dependencies=dependencies,
                events=existing_events,
            )

            try:
                self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    def publish_rollback(
        self,
        order_id: Any,
        subprocess_id: Any,
        *,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Atomically begin a rollback for a reversible subprocess."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = Order.backend()
        with backend.transaction():
            order = Order.query().where(Order.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            subprocess = (
                OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess_id).one()
            )
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            existing_events = (
                OrderEvent.query().where(OrderEvent.c.order_id == order_id).all()
            )

            result = self.dispatcher.on_rollback(
                order=order,
                subprocess=subprocess,
                events=existing_events,
                event_key=event_key,
            )

            try:
                self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    @staticmethod
    def _persist(result: DispatchResult, *, order: Order, subprocess: OrderSubProcess) -> None:
        """Save every mutated object produced by the dispatcher."""
        if subprocess.is_dirty:
            subprocess.save()
        for started in result.started_subprocesses:
            if started.is_dirty:
                started.save()
        if order.is_dirty:
            order.save()
        result.event.save()
        for outbox in result.outbox_items:
            outbox.save()


class AsyncOrderService:
    """Asynchronous service using async models with native ``await`` throughout.

    This is the genuine async counterpart of :class:`SyncOrderService` — not a
    thread-bridge wrapper. Every DB operation (``save``, ``query().all()``,
    ``transaction()``) is a coroutine provided by ``AsyncActiveRecord`` +
    ``AsyncStorageBackend``.
    """

    def __init__(self, dispatcher: Optional[AsyncOrderDispatcher] = None) -> None:
        self.dispatcher = dispatcher or AsyncOrderDispatcher()

    async def publish_event(
        self,
        order_id: Any,
        subprocess_id: Any,
        *,
        new_status: str,
        payload: Optional[Dict] = None,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Atomically load → dispatch → persist a subprocess status transition."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = AsyncOrder.backend()
        async with backend.transaction():
            order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            seed = await AsyncOrderSubProcess.query().where(
                AsyncOrderSubProcess.c.id == subprocess_id
            ).one()
            if seed is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            process_id = seed.process_id
            subprocesses = await AsyncOrderSubProcess.query().where(
                AsyncOrderSubProcess.c.process_id == process_id
            ).all()
            subprocess = next(
                (sp for sp in subprocesses if sp.id == subprocess_id), None
            )
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found in process {process_id}")

            dependencies = await AsyncSubProcessDependency.query().where(
                AsyncSubProcessDependency.c.process_id == process_id
            ).all()
            existing_events = await AsyncOrderEvent.query().where(
                AsyncOrderEvent.c.order_id == order_id
            ).all()

            result = await self.dispatcher.on_event(
                order=order,
                subprocess=subprocess,
                new_status=new_status,
                subprocesses=subprocesses,
                dependencies=dependencies,
                events=existing_events,
                event_key=event_key,
                payload=payload,
            )

            if result.duplicate:
                return result

            try:
                await self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    async def publish_timeout(
        self,
        order_id: Any,
        subprocess_id: Any,
    ) -> DispatchResult:
        """Atomically apply a subprocess timeout transition."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = AsyncOrder.backend()
        async with backend.transaction():
            order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            seed = await AsyncOrderSubProcess.query().where(
                AsyncOrderSubProcess.c.id == subprocess_id
            ).one()
            if seed is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            process_id = seed.process_id
            subprocesses = await AsyncOrderSubProcess.query().where(
                AsyncOrderSubProcess.c.process_id == process_id
            ).all()
            subprocess = next(
                (sp for sp in subprocesses if sp.id == subprocess_id), None
            )
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found in process {process_id}")

            dependencies = await AsyncSubProcessDependency.query().where(
                AsyncSubProcessDependency.c.process_id == process_id
            ).all()
            existing_events = await AsyncOrderEvent.query().where(
                AsyncOrderEvent.c.order_id == order_id
            ).all()

            result = await self.dispatcher.on_timeout(
                order=order,
                subprocess=subprocess,
                subprocesses=subprocesses,
                dependencies=dependencies,
                events=existing_events,
            )

            try:
                await self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    async def publish_rollback(
        self,
        order_id: Any,
        subprocess_id: Any,
        *,
        event_key: Optional[str] = None,
    ) -> DispatchResult:
        """Atomically begin a rollback for a reversible subprocess."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        backend = AsyncOrder.backend()
        async with backend.transaction():
            order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
            if order is None:
                raise ValueError(f"Order {order_id} not found")

            subprocess = await AsyncOrderSubProcess.query().where(
                AsyncOrderSubProcess.c.id == subprocess_id
            ).one()
            if subprocess is None:
                raise ValueError(f"SubProcess {subprocess_id} not found")

            existing_events = await AsyncOrderEvent.query().where(
                AsyncOrderEvent.c.order_id == order_id
            ).all()

            result = await self.dispatcher.on_rollback(
                order=order,
                subprocess=subprocess,
                events=existing_events,
                event_key=event_key,
            )

            try:
                await self._persist(result, order=order, subprocess=subprocess)
            except DatabaseError as exc:
                if _CONCURRENCY_MESSAGE in str(exc):
                    raise ConcurrentStateTransitionError(str(exc)) from exc
                raise

            return result

    @staticmethod
    async def _persist(result: DispatchResult, *, order: AsyncOrder, subprocess: AsyncOrderSubProcess) -> None:
        """Save every mutated object produced by the dispatcher."""
        if subprocess.is_dirty:
            await subprocess.save()
        for started in result.started_subprocesses:
            if started.is_dirty:
                await started.save()
        if order.is_dirty:
            await order.save()
        await result.event.save()
        for outbox in result.outbox_items:
            await outbox.save()


__all__ = [
    "AsyncOrderService",
    "SyncOrderService",
]
