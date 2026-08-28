# src/rhosocial/stateflow/timer.py
"""Timeout sweeper: scans for due subprocesses and publishes their timeout.

Sync and async schedulers are **fully parallel and non-interoperable**: the
sync scheduler uses ``OrderSubProcess`` with synchronous queries; the async
scheduler uses ``AsyncOrderSubProcess`` with native ``await``. No
``asyncio.to_thread`` is used.

Retry semantics
---------------
Each timeout uses a deterministic ``event_key`` (``timeout:{subprocess_id}``)
so ``publish_timeout`` is idempotent:

- A successful timeout, if re-scanned later, returns ``duplicate=True`` and is
  skipped (the subprocess is now terminal with a past ``timeout_at``).
- A failed timeout is rescheduled with exponential backoff (``timeout_at`` is
  reset into the future), so the next sweep retries it instead of hot-looping.
"""

from datetime import datetime, timedelta, timezone
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


def _timeout_event_key(subprocess_id: Any) -> str:
    """Deterministic idempotency key for a subprocess timeout."""
    return f"timeout:{subprocess_id}"


class SyncTimeoutScheduler:
    """Synchronous timeout sweeper backed by :class:`SyncOrderService`."""

    def __init__(
        self,
        service: Any,
        *,
        retry_base_delay: float = 60.0,
        retry_max_delay: float = 3600.0,
    ) -> None:
        from .service import SyncOrderService

        if not isinstance(service, SyncOrderService):
            raise TypeError("SyncTimeoutScheduler requires a SyncOrderService instance")
        self.service = service
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay

    def _retry_delay(self, attempt: int) -> timedelta:
        """Exponential backoff: base * 2^attempt, capped at max."""
        delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
        return timedelta(seconds=delay)

    def _reschedule(self, subprocess: OrderSubProcess, moment: datetime) -> None:
        """Reset timeout_at into the future with exponential backoff.

        Note: extra is reassigned (not mutated in place) because
        ActiveRecord dirty-tracking only detects field reassignment, not
        in-place dict mutation.
        """
        attempt = subprocess.extra.get("timeout_retry_count", 0) + 1
        subprocess.extra = {**subprocess.extra, "timeout_retry_count": attempt}
        subprocess.timeout_at = moment + self._retry_delay(attempt)
        subprocess.save()

    def _clear_retry_state(self, subprocess: OrderSubProcess) -> None:
        if "timeout_retry_count" in subprocess.extra:
            subprocess.extra = {k: v for k, v in subprocess.extra.items() if k != "timeout_retry_count"}
            subprocess.save()

    def tick(self, *, now: Optional[datetime] = None, limit: Optional[int] = None) -> int:
        """Sweep once and publish timeouts for all due subprocesses.

        Returns the number of **new** timeout transitions applied (duplicates
        and failures are not counted).
        """
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
                result = self.service.publish_timeout(
                    order_id=_order_id_for(subprocess.process_id),
                    subprocess_id=subprocess.id,
                    event_key=_timeout_event_key(subprocess.id),
                )
            except Exception:
                # Transient failure: reschedule with backoff, retry next sweep.
                self._reschedule(subprocess, moment)
                continue
            if result.duplicate:
                # Already timed out (terminal); skip without counting.
                continue
            self._clear_retry_state(subprocess)
            processed += 1
        return processed


class AsyncTimeoutScheduler:
    """Asynchronous timeout sweeper using async models with native ``await``."""

    def __init__(
        self,
        service: Any,
        *,
        retry_base_delay: float = 60.0,
        retry_max_delay: float = 3600.0,
    ) -> None:
        from .service import AsyncOrderService

        if not isinstance(service, AsyncOrderService):
            raise TypeError("AsyncTimeoutScheduler requires an AsyncOrderService instance")
        self.service = service
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay

    def _retry_delay(self, attempt: int) -> timedelta:
        delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
        return timedelta(seconds=delay)

    async def _reschedule(self, subprocess: AsyncOrderSubProcess, moment: datetime) -> None:
        attempt = subprocess.extra.get("timeout_retry_count", 0) + 1
        subprocess.extra = {**subprocess.extra, "timeout_retry_count": attempt}
        subprocess.timeout_at = moment + self._retry_delay(attempt)
        await subprocess.save()

    async def _clear_retry_state(self, subprocess: AsyncOrderSubProcess) -> None:
        if "timeout_retry_count" in subprocess.extra:
            subprocess.extra = {k: v for k, v in subprocess.extra.items() if k != "timeout_retry_count"}
            await subprocess.save()

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
                result = await self.service.publish_timeout(
                    order_id=order_id,
                    subprocess_id=subprocess.id,
                    event_key=_timeout_event_key(subprocess.id),
                )
            except Exception:
                await self._reschedule(subprocess, moment)
                continue
            if result.duplicate:
                continue
            await self._clear_retry_state(subprocess)
            processed += 1
        return processed


__all__ = [
    "AsyncTimeoutScheduler",
    "SyncTimeoutScheduler",
]
