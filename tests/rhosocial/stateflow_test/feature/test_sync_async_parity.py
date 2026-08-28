# tests/rhosocial/stateflow_test/feature/test_sync_async_parity.py
"""Sync/async parity tests.

Beyond matching method names, the **signature structure** (parameter names,
order, kinds, and defaults) must be identical between sync and async classes,
so call sites are interchangeable. Type annotations may differ where they
reference sync vs async model classes (e.g. ``OrderOutbox`` vs
``AsyncOrderOutbox``) — that is the unavoidable consequence of the
non-interoperability principle.
"""

import inspect
from typing import Any, Callable, List

from rhosocial.stateflow import (
    AsyncHandlerRollbackTopic,
    AsyncHandlerStartTopic,
    AsyncOrderDispatcher,
    AsyncOrderFactory,
    AsyncOrderOutboxDeliverer,
    AsyncOrderService,
    AsyncSubProcessHandler,
    AsyncTimeoutScheduler,
    SyncHandlerRollbackTopic,
    SyncHandlerStartTopic,
    SyncOrderDispatcher,
    SyncOrderFactory,
    SyncOrderOutboxDeliverer,
    SyncOrderService,
    SyncSubProcessHandler,
    SyncTimeoutScheduler,
)


# ---------------------------------------------------------------------------
# Pairs under test
# ---------------------------------------------------------------------------

PAIRS = [
    (SyncOrderFactory, AsyncOrderFactory),
    (SyncOrderDispatcher, AsyncOrderDispatcher),
    (SyncOrderService, AsyncOrderService),
    (SyncOrderOutboxDeliverer, AsyncOrderOutboxDeliverer),
    (SyncTimeoutScheduler, AsyncTimeoutScheduler),
    (SyncSubProcessHandler, AsyncSubProcessHandler),
    (SyncHandlerStartTopic, AsyncHandlerStartTopic),
    (SyncHandlerRollbackTopic, AsyncHandlerRollbackTopic),
]

# Topic classes have a ``topic`` class attribute (not a method) — excluded.
_PUBLIC = lambda cls: {  # noqa: E731
    name
    for name, value in inspect.getmembers(cls, predicate=inspect.isfunction)
    if not name.startswith("_") and name != "topic"
}

# Members that are pure registration/configuration or shared pure helpers
# (no I/O) — the async counterpart is intentionally NOT a coroutine.
_NON_ASYNC_MEMBERS = {
    "register_topic_handler",  # stores a callable, no I/O
    "retry_delay",  # pure arithmetic
    "timeout_event_key",  # pure string formatting
}


def public_methods(cls):
    """Return the set of public method names on ``cls``."""
    return _PUBLIC(cls)


def signature_structure(method: Callable) -> List[Any]:
    """Return the structural signature: (name, kind, default) per parameter.

    Excludes annotations so sync/async model type differences are ignored.
    """
    sig = inspect.signature(method)
    structure = []
    for name, param in sig.parameters.items():
        structure.append((name, param.kind.name, param.default))
    return structure


def test_paired_classes_have_identical_public_methods():
    """Sync and async classes expose the same set of public method names."""
    for sync_cls, async_cls in PAIRS:
        assert public_methods(sync_cls) == public_methods(async_cls), (
            f"Method names differ between {sync_cls.__name__} and {async_cls.__name__}"
        )


def test_paired_methods_have_identical_signature_structure():
    """Every paired method has the same parameter names, order, kinds, defaults.

    Type annotations may differ (sync vs async model classes) but the call
    signature is interchangeable.
    """
    for sync_cls, async_cls in PAIRS:
        for method_name in sorted(public_methods(sync_cls)):
            sync_sig = signature_structure(getattr(sync_cls, method_name))
            async_sig = signature_structure(getattr(async_cls, method_name))
            assert sync_sig == async_sig, (
                f"Signature structure differs for {sync_cls.__name__}.{method_name}\n"
                f"  sync : {sync_sig}\n"
                f"  async: {async_sig}"
            )


def test_paired_methods_have_matching_return_kinds():
    """Sync methods return values directly; async counterparts are coroutines.

    Pure registration/configuration members (e.g. ``register_topic_handler``)
    are exempt — they perform no I/O and the async variant is intentionally
    not a coroutine.
    """
    for sync_cls, async_cls in PAIRS:
        for method_name in sorted(public_methods(sync_cls)):
            sync_m = getattr(sync_cls, method_name)
            async_m = getattr(async_cls, method_name)
            assert not inspect.iscoroutinefunction(sync_m), (
                f"{sync_cls.__name__}.{method_name} should not be a coroutine"
            )
            if method_name in _NON_ASYNC_MEMBERS:
                assert not inspect.iscoroutinefunction(async_m), (
                    f"{async_cls.__name__}.{method_name} should not be a coroutine (pure registration)"
                )
            else:
                assert inspect.iscoroutinefunction(async_m), (
                    f"{async_cls.__name__}.{method_name} should be a coroutine"
                )


def test_async_methods_are_coroutines():
    """Explicit check that I/O async members are coroutine functions."""
    for _, async_cls in PAIRS:
        for method_name in sorted(public_methods(async_cls)):
            if method_name in _NON_ASYNC_MEMBERS:
                continue
            assert inspect.iscoroutinefunction(getattr(async_cls, method_name)), (
                f"{async_cls.__name__}.{method_name} should be a coroutine"
            )
