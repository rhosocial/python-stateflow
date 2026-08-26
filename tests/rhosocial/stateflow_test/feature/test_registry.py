# tests/rhosocial/stateflow_test/feature/test_registry.py
"""Handler registry and handler_start topic tests."""

import uuid

import pytest

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    AsyncSubProcessHandler,
    FlowPath,
    HandlerRegistry,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
    SyncHandlerStartTopic,
    SyncOrderFactory,
    SyncOrderOutboxDeliverer,
    SyncOrderService,
    SyncSubProcessHandler,
    UnknownHandlerError,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.types import (
    OUTBOX_TOPIC_HANDLER_START,
)

from .helpers import make_steps

ALL_MODELS = (
    OrderTemplate, OrderTemplateStep, FlowPath, Order, OrderProcess,
    OrderSubProcess, SubProcessDependency, OrderEvent, OrderOutbox,
)


@pytest.fixture
def backend_group():
    config = SQLiteConnectionConfig(database=":memory:", check_same_thread=False)
    with BackendGroup(
        name="registry-test", models=list(ALL_MODELS), config=config, backend_class=SQLiteBackend,
    ) as group:
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        yield group
        drop_tables(backend)


@pytest.fixture
def persisted_instance(backend_group):
    template, steps = make_steps()
    template.save()
    for step in steps:
        step.save()
    instance = SyncOrderFactory().create(template, steps, context={"user_id": "u1"})
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()
    return instance


class _RecordingSyncHandler(SyncSubProcessHandler):
    """Sync handler that reports a fixed status and records call count."""

    instances_created = 0
    start_calls = 0

    def __init__(self, subprocess):
        super().__init__(subprocess)
        type(self).instances_created += 1

    def start(self):
        type(self).start_calls += 1
        from rhosocial.stateflow import HandlerResult
        return HandlerResult(status="locked", event_key=f"start-{self.subprocess.id}")

    def rollback(self):
        return None


class _RecordingAsyncHandler(AsyncSubProcessHandler):
    """Async handler mirroring the sync recorder."""

    instances_created = 0
    start_calls = 0

    def __init__(self, subprocess):
        super().__init__(subprocess)
        type(self).instances_created += 1

    async def start(self):
        type(self).start_calls += 1
        from rhosocial.stateflow import HandlerResult
        return HandlerResult(status="locked", event_key=f"start-{self.subprocess.id}")

    async def rollback(self):
        return None


def test_registry_resolve_returns_none_for_unregistered():
    reg = HandlerRegistry()
    assert reg.resolve("missing.Handler") is None


def test_registry_register_and_resolve_round_trip():
    reg = HandlerRegistry()
    reg.register("my.Handler", _RecordingSyncHandler)
    assert reg.resolve("my.Handler") is _RecordingSyncHandler


def test_registry_instantiate_raises_for_unknown_key():
    reg = HandlerRegistry()
    with pytest.raises(UnknownHandlerError):
        reg.instantiate("nope", subprocess=object())


def test_registry_dynamic_import_opt_in():
    """Dynamic import only happens when allow_dynamic_import is True."""
    reg = HandlerRegistry()  # default: dynamic import disabled
    assert reg.resolve("os.path.basename") is None

    reg = HandlerRegistry(allow_dynamic_import=True)
    resolved = reg.resolve("os.path.basename")
    import os
    assert resolved is os.path.basename


def test_sync_handler_start_topic_drives_subprocess_to_completion(
    backend_group, persisted_instance
):
    """The topic resolves the handler, calls start(), and publishes the result status."""
    _RecordingSyncHandler.instances_created = 0
    _RecordingSyncHandler.start_calls = 0

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", _RecordingSyncHandler)
    service = SyncOrderService()
    topic = SyncHandlerStartTopic(registry, service)

    inventory = persisted_instance.get_subprocess("inventory")
    outbox_item = OrderOutbox(
        event_id=uuid.uuid4(),
        topic=OUTBOX_TOPIC_HANDLER_START,
        payload={"subprocess_id": str(inventory.id)},
    )
    outbox_item.save()

    ok = topic(outbox_item)

    assert ok is True
    assert _RecordingSyncHandler.instances_created == 1
    assert _RecordingSyncHandler.start_calls == 1

    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.status == "locked"
    assert reloaded.completed_at is not None

    # The outbox status is managed by the deliverer, not the topic handler.
    # When the topic is called directly the outbox row stays in its initial
    # pending state; the deliverer tests cover the pending → sent transition.


def test_sync_handler_start_topic_marks_failed_when_handler_unknown(
    backend_group, persisted_instance
):
    """An unresolvable handler_class surfaces as a failed outbox item."""
    registry = HandlerRegistry()
    service = SyncOrderService()
    topic = SyncHandlerStartTopic(registry, service)

    inventory = persisted_instance.get_subprocess("inventory")
    outbox_item = OrderOutbox(
        event_id=uuid.uuid4(),
        topic=OUTBOX_TOPIC_HANDLER_START,
        payload={"subprocess_id": str(inventory.id)},
    )
    outbox_item.save()

    deliverer = SyncOrderOutboxDeliverer()
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, topic)
    deliverer.deliver_pending()

    from rhosocial.stateflow.types import OUTBOX_STATUS_FAILED
    failed = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_FAILED).all()
    assert len(failed) == 1


def test_sync_handler_start_topic_skips_publish_when_handler_returns_none(
    backend_group, persisted_instance
):
    """A handler returning None (no result) leaves the subprocess untouched and still marks sent."""

    class NoOpHandler(SyncSubProcessHandler):
        def start(self):
            return None

        def rollback(self):
            return None

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", NoOpHandler)
    service = SyncOrderService()
    topic = SyncHandlerStartTopic(registry, service)

    inventory = persisted_instance.get_subprocess("inventory")
    outbox_item = OrderOutbox(
        event_id=uuid.uuid4(),
        topic=OUTBOX_TOPIC_HANDLER_START,
        payload={"subprocess_id": str(inventory.id)},
    )
    outbox_item.save()

    ok = topic(outbox_item)

    assert ok is True
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    # The dispatcher was never invoked here (we built the outbox item manually),
    # so a None-returning handler leaves the subprocess in its initial state.
    assert reloaded.status == "pending"

