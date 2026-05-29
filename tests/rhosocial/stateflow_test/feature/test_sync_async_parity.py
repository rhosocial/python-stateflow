# tests/rhosocial/stateflow_test/feature/test_sync_async_parity.py

import inspect

from rhosocial.stateflow import (
    AsyncOrderDispatcher,
    AsyncOrderFactory,
    AsyncSubProcessHandler,
    SyncOrderDispatcher,
    SyncOrderFactory,
    SyncSubProcessHandler,
)


def public_methods(cls):
    return {
        name
        for name, value in inspect.getmembers(cls, predicate=inspect.isfunction)
        if not name.startswith("_")
    }


def test_factory_method_names_are_paired():
    assert public_methods(SyncOrderFactory) == public_methods(AsyncOrderFactory)


def test_dispatcher_method_names_are_paired():
    assert public_methods(SyncOrderDispatcher) == public_methods(AsyncOrderDispatcher)


def test_handler_method_names_are_paired():
    assert public_methods(SyncSubProcessHandler) == public_methods(AsyncSubProcessHandler)


def test_async_methods_are_coroutines():
    assert inspect.iscoroutinefunction(AsyncOrderFactory.create)
    assert inspect.iscoroutinefunction(AsyncOrderFactory.append_subprocess)
    assert inspect.iscoroutinefunction(AsyncOrderDispatcher.on_event)
    assert inspect.iscoroutinefunction(AsyncOrderDispatcher.on_timeout)
    assert inspect.iscoroutinefunction(AsyncSubProcessHandler.start)
    assert inspect.iscoroutinefunction(AsyncSubProcessHandler.rollback)
