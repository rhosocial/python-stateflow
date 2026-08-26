# src/rhosocial/stateflow/registry.py
"""Handler registry and the default ``handler_start`` / ``handler_rollback`` topics.

Sync and async topic handlers are **fully parallel and non-interoperable**:
sync handlers use sync models (``Order``, ``OrderSubProcess``, …) with
synchronous ``save()`` / ``query()``; async handlers use async models
(``AsyncOrder``, ``AsyncOrderSubProcess``, …) with native ``await``. No
``asyncio.to_thread`` is used.
"""

import importlib
from typing import Any, Dict, Optional, Type, Union

from .exceptions import StateflowError
from .handlers import AsyncSubProcessHandler, SyncSubProcessHandler
from .models import (
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderOutbox,
    AsyncOrderProcess,
    AsyncOrderSubProcess,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
)
from .types import OUTBOX_TOPIC_HANDLER_ROLLBACK, OUTBOX_TOPIC_HANDLER_START

HandlerCls = Union[Type[SyncSubProcessHandler], Type[AsyncSubProcessHandler]]


class UnknownHandlerError(StateflowError):
    """Raised when a handler_class string cannot be resolved to a handler."""


class HandlerRegistry:
    """Maps ``handler_class`` strings to handler classes.

    Resolution order:
    1. Explicit ``register(key, cls)`` entries.
    2. Optional ``importlib`` lookup for ``"module.attr"`` style keys when
       ``allow_dynamic_import`` is enabled at construction time.
    """

    def __init__(self, *, allow_dynamic_import: bool = False) -> None:
        self._registry: Dict[str, HandlerCls] = {}
        self._allow_dynamic_import = allow_dynamic_import

    def register(self, key: str, handler_cls: Any) -> None:
        """Bind a handler_class string to a concrete handler class."""
        self._registry[key] = handler_cls

    def resolve(self, key: str) -> Optional[HandlerCls]:
        """Return the handler class for ``key`` or ``None`` if unresolved."""
        explicit = self._registry.get(key)
        if explicit is not None:
            return explicit
        if self._allow_dynamic_import and "." in key:
            module_path, _, attr = key.rpartition(".")
            try:
                module = importlib.import_module(module_path)
            except ImportError:
                return None
            return getattr(module, attr, None)
        return None

    def instantiate(self, key: str, subprocess: OrderSubProcess) -> SyncSubProcessHandler:
        """Resolve and construct a sync handler bound to ``subprocess``."""
        cls = self.resolve(key)
        if cls is None:
            raise UnknownHandlerError(f"No handler registered for '{key}'")
        instance = cls(subprocess)
        if not isinstance(instance, SyncSubProcessHandler):
            raise UnknownHandlerError(
                f"Handler '{key}' resolved to {type(instance).__name__} which is not a SyncSubProcessHandler"
            )
        return instance

    def instantiate_async(self, key: str, subprocess: AsyncOrderSubProcess) -> AsyncSubProcessHandler:
        """Resolve and construct an async handler bound to ``subprocess``."""
        cls = self.resolve(key)
        if cls is None:
            raise UnknownHandlerError(f"No handler registered for '{key}'")
        instance = cls(subprocess)
        if not isinstance(instance, AsyncSubProcessHandler):
            raise UnknownHandlerError(
                f"Handler '{key}' resolved to {type(instance).__name__} which is not an AsyncSubProcessHandler"
            )
        return instance


# ---------------------------------------------------------------------------
# Subprocess loaders (sync + async)
# ---------------------------------------------------------------------------


def _load_subprocess_for_outbox(outbox_item: OrderOutbox) -> tuple[OrderSubProcess, Any]:
    """Load the subprocess referenced by an outbox payload, plus its order_id."""
    subprocess_id = outbox_item.payload.get("subprocess_id")
    if subprocess_id is None:
        raise UnknownHandlerError("Outbox payload is missing 'subprocess_id'")
    subprocess = (
        OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess_id).one()
    )
    if subprocess is None:
        raise UnknownHandlerError(f"SubProcess {subprocess_id} not found")
    process = OrderProcess.query().where(OrderProcess.c.id == subprocess.process_id).one()
    if process is None:
        raise UnknownHandlerError(f"OrderProcess {subprocess.process_id} not found")
    return subprocess, process.order_id


async def _async_load_subprocess_for_outbox(outbox_item: AsyncOrderOutbox) -> tuple[AsyncOrderSubProcess, Any]:
    """Load the subprocess referenced by an outbox payload, plus its order_id (async)."""
    subprocess_id = outbox_item.payload.get("subprocess_id")
    if subprocess_id is None:
        raise UnknownHandlerError("Outbox payload is missing 'subprocess_id'")
    subprocess = await (
        AsyncOrderSubProcess.query().where(AsyncOrderSubProcess.c.id == subprocess_id).one()
    )
    if subprocess is None:
        raise UnknownHandlerError(f"SubProcess {subprocess_id} not found")
    process = await AsyncOrderProcess.query().where(AsyncOrderProcess.c.id == subprocess.process_id).one()
    if process is None:
        raise UnknownHandlerError(f"OrderProcess {subprocess.process_id} not found")
    return subprocess, process.order_id


# ---------------------------------------------------------------------------
# handler_start topic
# ---------------------------------------------------------------------------


class SyncHandlerStartTopic:
    """Default ``handler_start`` topic callable for the sync deliverer.

    Resolves the subprocess's ``handler_class``, calls ``start()``, and feeds
    the returned :class:`~rhosocial.stateflow.types.HandlerResult` back through
    :class:`~rhosocial.stateflow.service.SyncOrderService`.
    """

    topic = OUTBOX_TOPIC_HANDLER_START

    def __init__(self, registry: HandlerRegistry, service: Any) -> None:
        self.registry = registry
        self.service = service

    def __call__(self, outbox_item: OrderOutbox) -> bool:
        from .deliverer import UnrecoverableDeliveryError
        from .service import SyncOrderService

        if not isinstance(self.service, SyncOrderService):
            raise TypeError("SyncHandlerStartTopic requires a SyncOrderService instance")
        try:
            subprocess, order_id = _load_subprocess_for_outbox(outbox_item)
            handler = self.registry.instantiate(subprocess.handler_class, subprocess)
        except UnknownHandlerError as exc:
            raise UnrecoverableDeliveryError(str(exc)) from exc
        result = handler.start()
        if result is not None and result.status:
            self.service.publish_event(
                order_id=order_id,
                subprocess_id=subprocess.id,
                new_status=result.status,
                payload=result.payload,
                event_key=result.event_key,
            )
        return True


class AsyncHandlerStartTopic:
    """Default ``handler_start`` topic callable for the async deliverer.

    Uses async models with native ``await`` for all DB operations.
    """

    topic = OUTBOX_TOPIC_HANDLER_START

    def __init__(self, registry: HandlerRegistry, service: Any) -> None:
        self.registry = registry
        self.service = service

    async def __call__(self, outbox_item: AsyncOrderOutbox) -> bool:
        from .deliverer import UnrecoverableDeliveryError
        from .service import AsyncOrderService

        if not isinstance(self.service, AsyncOrderService):
            raise TypeError("AsyncHandlerStartTopic requires an AsyncOrderService instance")
        try:
            subprocess, order_id = await _async_load_subprocess_for_outbox(outbox_item)
            handler = self.registry.instantiate_async(subprocess.handler_class, subprocess)
        except UnknownHandlerError as exc:
            raise UnrecoverableDeliveryError(str(exc)) from exc
        result = await handler.start()
        if result is not None and result.status:
            await self.service.publish_event(
                order_id=order_id,
                subprocess_id=subprocess.id,
                new_status=result.status,
                payload=result.payload,
                event_key=result.event_key,
            )
        return True


# ---------------------------------------------------------------------------
# handler_rollback topic
# ---------------------------------------------------------------------------


class SyncHandlerRollbackTopic:
    """Default ``handler_rollback`` topic callable for the sync deliverer.

    Resolves the handler, calls ``rollback()``, publishes the result status
    through :class:`~rhosocial.stateflow.service.SyncOrderService`, and marks
    the subprocess's ``rollback_status`` as ``completed`` or ``failed``.
    """

    topic = OUTBOX_TOPIC_HANDLER_ROLLBACK

    def __init__(self, registry: HandlerRegistry, service: Any) -> None:
        self.registry = registry
        self.service = service

    def __call__(self, outbox_item: OrderOutbox) -> bool:
        from .deliverer import UnrecoverableDeliveryError
        from .service import SyncOrderService

        if not isinstance(self.service, SyncOrderService):
            raise TypeError("SyncHandlerRollbackTopic requires a SyncOrderService instance")
        try:
            subprocess, order_id = _load_subprocess_for_outbox(outbox_item)
            handler = self.registry.instantiate(subprocess.handler_class, subprocess)
        except UnknownHandlerError as exc:
            raise UnrecoverableDeliveryError(str(exc)) from exc

        try:
            result = handler.rollback()
        except Exception as exc:
            error_payload = {"error": str(exc), "type": type(exc).__name__}
            self._mark_failed(subprocess, order_id, error_payload)
            return True

        if result is not None and result.status:
            self.service.publish_event(
                order_id=order_id,
                subprocess_id=subprocess.id,
                new_status=result.status,
                payload=result.payload,
                event_key=result.event_key,
            )

        subprocess = (
            OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess.id).one()
        )
        subprocess.complete_rollback()
        subprocess.save()
        return True

    @staticmethod
    def _mark_failed(subprocess: OrderSubProcess, order_id: Any, error_payload: dict) -> None:
        """Record a rollback failure on the subprocess and persist it."""
        backend = OrderOutbox.backend()
        with backend.transaction():
            refreshed = (
                OrderSubProcess.query().where(OrderSubProcess.c.id == subprocess.id).one()
            )
            if refreshed is None:
                return
            refreshed.fail_rollback(error_payload)
            refreshed.save()
            order = Order.query().where(Order.c.id == order_id).one()
            if order is not None:
                failure_event = OrderEvent.rollback_failed(order, refreshed, error_payload)
                failure_event.save()


class AsyncHandlerRollbackTopic:
    """Default ``handler_rollback`` topic callable for the async deliverer.

    Uses async models with native ``await`` for all DB operations.
    """

    topic = OUTBOX_TOPIC_HANDLER_ROLLBACK

    def __init__(self, registry: HandlerRegistry, service: Any) -> None:
        self.registry = registry
        self.service = service

    async def __call__(self, outbox_item: AsyncOrderOutbox) -> bool:
        from .deliverer import UnrecoverableDeliveryError
        from .service import AsyncOrderService

        if not isinstance(self.service, AsyncOrderService):
            raise TypeError("AsyncHandlerRollbackTopic requires an AsyncOrderService instance")
        try:
            subprocess, order_id = await _async_load_subprocess_for_outbox(outbox_item)
            handler = self.registry.instantiate_async(subprocess.handler_class, subprocess)
        except UnknownHandlerError as exc:
            raise UnrecoverableDeliveryError(str(exc)) from exc

        try:
            result = await handler.rollback()
        except Exception as exc:
            error_payload = {"error": str(exc), "type": type(exc).__name__}
            await self._mark_failed(subprocess, order_id, error_payload)
            return True

        if result is not None and result.status:
            await self.service.publish_event(
                order_id=order_id,
                subprocess_id=subprocess.id,
                new_status=result.status,
                payload=result.payload,
                event_key=result.event_key,
            )

        subprocess = await (
            AsyncOrderSubProcess.query().where(AsyncOrderSubProcess.c.id == subprocess.id).one()
        )
        subprocess.complete_rollback()
        await subprocess.save()
        return True

    @staticmethod
    async def _mark_failed(subprocess: AsyncOrderSubProcess, order_id: Any, error_payload: dict) -> None:
        """Record a rollback failure on the async subprocess and persist it."""
        backend = AsyncOrderOutbox.backend()
        async with backend.transaction():
            refreshed = await (
                AsyncOrderSubProcess.query().where(AsyncOrderSubProcess.c.id == subprocess.id).one()
            )
            if refreshed is None:
                return
            refreshed.fail_rollback(error_payload)
            await refreshed.save()
            order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
            if order is not None:
                failure_event = AsyncOrderEvent.rollback_failed(order, refreshed, error_payload)
                await failure_event.save()


__all__ = [
    "AsyncHandlerRollbackTopic",
    "AsyncHandlerStartTopic",
    "HandlerRegistry",
    "SyncHandlerRollbackTopic",
    "SyncHandlerStartTopic",
    "UnknownHandlerError",
]
