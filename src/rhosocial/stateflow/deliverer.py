# src/rhosocial/stateflow/deliverer.py
"""Outbox deliverer: an independent loop that drains ``OrderOutbox`` rows.

Sync and async deliverers are **fully parallel and non-interoperable**: the
sync deliverer uses ``OrderOutbox`` with synchronous ``save()`` / ``query()``
/ ``backend.transaction()``; the async deliverer uses ``AsyncOrderOutbox``
with native ``await`` at every DB call site. No ``asyncio.to_thread``
bridging is used.

Topic handlers are plain callables registered against the outbox ``topic``
string. The ``handler_start`` topic is wired up by the handler registry in
:mod:`rhosocial.stateflow.registry`; the deliverer itself stays agnostic so
users can plug in ``notification``, ``timer`` or any custom topic.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Optional

from .exceptions import StateflowError
from .models import AsyncOrderOutbox, OrderOutbox
from .types import (
    OUTBOX_STATUS_FAILED,
    OUTBOX_STATUS_PENDING,
    OUTBOX_STATUS_PROCESSING,
    OUTBOX_STATUS_SENT,
)

TopicHandler = Callable[[OrderOutbox], bool]
AsyncTopicHandler = Callable[[AsyncOrderOutbox], Awaitable[bool]]


class UnrecoverableDeliveryError(StateflowError):
    """Raised by a topic handler to signal that the item should not be retried."""


class _DelivererBase:
    """Shared configuration and retry-policy logic for sync and async deliverers."""

    def __init__(
        self,
        *,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 300.0,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._topic_handlers: Dict[str, Any] = {}

    def _next_retry_at(self, retry_count: int) -> datetime:
        """Compute the next retry timestamp with exponential backoff."""
        delay = min(
            self._base_delay * (2 ** (retry_count - 1)),
            self._max_delay,
        )
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    def _exceeded_max_retries(self, retry_count: int) -> bool:
        return retry_count > self._max_retries


class SyncOrderOutboxDeliverer(_DelivererBase):
    """Synchronous outbox deliverer with retry and cancellation semantics."""

    def register_topic_handler(self, topic: str, handler: TopicHandler) -> None:
        """Register a sync topic handler returning ``True`` on success."""
        self._topic_handlers[topic] = handler

    def deliver_pending(self, limit: Optional[int] = None) -> int:
        """Drain up to ``limit`` pending outbox items. Returns the count processed."""
        backend = OrderOutbox.backend()
        processed = 0
        while limit is None or processed < limit:
            item = self._claim_next_pending(backend)
            if item is None:
                break
            processed += 1
            self._deliver_one(backend, item)
        return processed

    def recover_stuck(self, stuck_after: timedelta) -> int:
        """Move ``processing`` items older than ``stuck_after`` back to ``pending``."""
        backend = OrderOutbox.backend()
        cutoff = datetime.now(timezone.utc) - stuck_after
        with backend.transaction():
            stuck = (
                OrderOutbox.query()
                .where(OrderOutbox.c.status == OUTBOX_STATUS_PROCESSING)
                .where(OrderOutbox.c.updated_at <= cutoff)
                .all()
            )
            for item in stuck:
                item.status = OUTBOX_STATUS_PENDING
                item.save()
            return len(stuck)

    def _claim_next_pending(self, backend) -> Optional[OrderOutbox]:
        """Atomically claim the next due pending item (pending → processing)."""
        now = datetime.now(timezone.utc)
        with backend.transaction():
            fresh = (
                OrderOutbox.query()
                .where(OrderOutbox.c.status == OUTBOX_STATUS_PENDING)
                .where(OrderOutbox.c.next_retry_at.is_null())
                .all()
            )
            if fresh:
                item = fresh[0]
                item.status = OUTBOX_STATUS_PROCESSING
                item.save()
                return item

            retryable = (
                OrderOutbox.query()
                .where(OrderOutbox.c.status == OUTBOX_STATUS_PENDING)
                .where(OrderOutbox.c.next_retry_at <= now)
                .all()
            )
            if retryable:
                item = retryable[0]
                item.status = OUTBOX_STATUS_PROCESSING
                item.save()
                return item
            return None

    def _deliver_one(self, backend, item: OrderOutbox) -> None:
        """Invoke the topic handler and persist the outcome in one transaction."""
        handler = self._topic_handlers.get(item.topic)
        if handler is None:
            self._mark_failed(backend, item, f"No handler registered for topic '{item.topic}'")
            return

        try:
            ok = handler(item)
        except UnrecoverableDeliveryError as exc:
            self._mark_failed(backend, item, str(exc))
            return
        except Exception as exc:
            self._mark_retryable(backend, item, str(exc))
            return

        if ok:
            self._mark_sent(backend, item)
        else:
            self._mark_retryable(backend, item, "Topic handler returned False")

    def _mark_sent(self, backend, item: OrderOutbox) -> None:
        with backend.transaction():
            item.status = OUTBOX_STATUS_SENT
            item.next_retry_at = None
            item.save()

    def _mark_retryable(self, backend, item: OrderOutbox, reason: str) -> None:
        with backend.transaction():
            item.retry_count += 1
            if self._exceeded_max_retries(item.retry_count):
                item.status = OUTBOX_STATUS_FAILED
                item.next_retry_at = None
            else:
                item.status = OUTBOX_STATUS_PENDING
                item.next_retry_at = self._next_retry_at(item.retry_count)
            item.save()

    def _mark_failed(self, backend, item: OrderOutbox, reason: str) -> None:
        with backend.transaction():
            item.status = OUTBOX_STATUS_FAILED
            item.next_retry_at = None
            item.save()


class AsyncOrderOutboxDeliverer(_DelivererBase):
    """Asynchronous outbox deliverer using ``AsyncOrderOutbox`` with native ``await``.

    Every DB operation (``save``, ``query().all()``, ``transaction()``) is a
    coroutine provided by ``AsyncActiveRecord`` + ``AsyncStorageBackend``.
    No ``asyncio.to_thread`` is used.
    """

    def register_topic_handler(self, topic: str, handler: AsyncTopicHandler) -> None:
        """Register an async topic handler returning ``True`` on success."""
        self._topic_handlers[topic] = handler

    async def deliver_pending(self, limit: Optional[int] = None) -> int:
        """Drain up to ``limit`` pending outbox items asynchronously."""
        backend = AsyncOrderOutbox.backend()
        processed = 0
        while limit is None or processed < limit:
            item = await self._claim_next_pending(backend)
            if item is None:
                break
            processed += 1
            await self._deliver_one(backend, item)
        return processed

    async def recover_stuck(self, stuck_after: timedelta) -> int:
        """Move stranded ``processing`` items back to ``pending``."""
        backend = AsyncOrderOutbox.backend()
        cutoff = datetime.now(timezone.utc) - stuck_after
        async with backend.transaction():
            stuck = await (
                AsyncOrderOutbox.query()
                .where(AsyncOrderOutbox.c.status == OUTBOX_STATUS_PROCESSING)
                .where(AsyncOrderOutbox.c.updated_at <= cutoff)
                .all()
            )
            for item in stuck:
                item.status = OUTBOX_STATUS_PENDING
                await item.save()
            return len(stuck)

    async def _claim_next_pending(self, backend) -> Optional[AsyncOrderOutbox]:
        """Atomically claim the next due pending item (pending → processing)."""
        now = datetime.now(timezone.utc)
        async with backend.transaction():
            fresh = await (
                AsyncOrderOutbox.query()
                .where(AsyncOrderOutbox.c.status == OUTBOX_STATUS_PENDING)
                .where(AsyncOrderOutbox.c.next_retry_at.is_null())
                .all()
            )
            if fresh:
                item = fresh[0]
                item.status = OUTBOX_STATUS_PROCESSING
                await item.save()
                return item

            retryable = await (
                AsyncOrderOutbox.query()
                .where(AsyncOrderOutbox.c.status == OUTBOX_STATUS_PENDING)
                .where(AsyncOrderOutbox.c.next_retry_at <= now)
                .all()
            )
            if retryable:
                item = retryable[0]
                item.status = OUTBOX_STATUS_PROCESSING
                await item.save()
                return item
            return None

    async def _deliver_one(self, backend, item: AsyncOrderOutbox) -> None:
        """Invoke the async topic handler and persist the outcome."""
        handler = self._topic_handlers.get(item.topic)
        if handler is None:
            await self._mark_failed(backend, item, f"No handler registered for topic '{item.topic}'")
            return

        try:
            ok = await handler(item)
        except UnrecoverableDeliveryError as exc:
            await self._mark_failed(backend, item, str(exc))
            return
        except Exception as exc:
            await self._mark_retryable(backend, item, str(exc))
            return

        if ok:
            await self._mark_sent(backend, item)
        else:
            await self._mark_retryable(backend, item, "Topic handler returned False")

    async def _mark_sent(self, backend, item: AsyncOrderOutbox) -> None:
        async with backend.transaction():
            item.status = OUTBOX_STATUS_SENT
            item.next_retry_at = None
            await item.save()

    async def _mark_retryable(self, backend, item: AsyncOrderOutbox, reason: str) -> None:
        async with backend.transaction():
            item.retry_count += 1
            if self._exceeded_max_retries(item.retry_count):
                item.status = OUTBOX_STATUS_FAILED
                item.next_retry_at = None
            else:
                item.status = OUTBOX_STATUS_PENDING
                item.next_retry_at = self._next_retry_at(item.retry_count)
            await item.save()

    async def _mark_failed(self, backend, item: AsyncOrderOutbox, reason: str) -> None:
        async with backend.transaction():
            item.status = OUTBOX_STATUS_FAILED
            item.next_retry_at = None
            await item.save()


__all__ = [
    "AsyncOrderOutboxDeliverer",
    "SyncOrderOutboxDeliverer",
    "TopicHandler",
    "AsyncTopicHandler",
    "UnrecoverableDeliveryError",
]
