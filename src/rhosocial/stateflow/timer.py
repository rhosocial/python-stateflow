# src/rhosocial/stateflow/timer.py
"""Timeout sweeper: scans for due subprocesses and publishes their timeout.

Sync and async schedulers are **fully parallel and non-interoperable**: the
sync scheduler uses ``OrderSubProcess`` with synchronous queries; the async
scheduler uses ``AsyncOrderSubProcess`` with native ``await``. No
``asyncio.to_thread`` is used.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from .models import AsyncOrderProcess, AsyncOrderSubProcess, OrderProcess, OrderSubProcess


def _order_id_for(process_id: Any) -> Any:
    """Resolve the order_id for a process_id via the sync OrderProcess row."""
    process = OrderProcess.query().where(OrderProcess.c.id == process_id).one()
    if process is None:
        raise ValueError(f"OrderProcess {process_id} not found")
    return process.order_id


async def _async_order_id_for(process_id: Any) -> Any:
    """Resolve the order_id for a process_id via the async AsyncOrderProcess row."""
    process = await AsyncOrderProcess.query().where(AsyncOrderProcess.c.id == process_id).one()
    if process is None:
        raise ValueError(f"OrderProcess {process_id} not found")
    return process.order_id


class SyncTimeoutScheduler:
    """Synchronous timeout sweeper backed by :class:`SyncOrderService`."""

    def __init__(self, service: Any) -> None:
        from .service import SyncOrderService

        if not isinstance(service, SyncOrderService):
            raise TypeError("SyncTimeoutScheduler requires a SyncOrderService instance")
        self.service = service

    def tick(self, *, now: Optional[datetime] = None, limit: Optional[int] = None) -> int:
        """Sweep once and publish timeouts for all due subprocesses."""
        moment = now or datetime.now(timezone.utc)
        due = (
            OrderSubProcess.query()
            .where(OrderSubProcess.c.skipped == False)  # noqa: E712
            .where(OrderSubProcess.c.timeout_at <= moment)
            .all()
        )
        if limit is not None:
            due = due[:limit]
        processed = 0
        for subprocess in due:
            try:
                self.service.publish_timeout(
                    order_id=_order_id_for(subprocess.process_id),
                    subprocess_id=subprocess.id,
                )
            except Exception:
                continue
            processed += 1
        return processed


class AsyncTimeoutScheduler:
    """Asynchronous timeout sweeper using async models with native ``await``."""

    def __init__(self, service: Any) -> None:
        from .service import AsyncOrderService

        if not isinstance(service, AsyncOrderService):
            raise TypeError("AsyncTimeoutScheduler requires an AsyncOrderService instance")
        self.service = service

    async def tick(self, *, now: Optional[datetime] = None, limit: Optional[int] = None) -> int:
        """Sweep once and publish timeouts asynchronously."""
        moment = now or datetime.now(timezone.utc)
        due = await (
            AsyncOrderSubProcess.query()
            .where(AsyncOrderSubProcess.c.skipped == False)  # noqa: E712
            .where(AsyncOrderSubProcess.c.timeout_at <= moment)
            .all()
        )
        if limit is not None:
            due = due[:limit]
        processed = 0
        for subprocess in due:
            try:
                order_id = await _async_order_id_for(subprocess.process_id)
                await self.service.publish_timeout(
                    order_id=order_id,
                    subprocess_id=subprocess.id,
                )
            except Exception:
                continue
            processed += 1
        return processed


__all__ = [
    "AsyncTimeoutScheduler",
    "SyncTimeoutScheduler",
]
