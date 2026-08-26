# tests/rhosocial/stateflow_test/feature/test_rollback.py
"""Rollback lifecycle tests: begin → handler.rollback → complete/fail."""


import pytest

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    FlowPath,
    HandlerRegistry,
    HandlerResult,
    InvalidStateTransitionError,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
    SyncHandlerRollbackTopic,
    SyncOrderFactory,
    SyncOrderService,
    SyncSubProcessHandler,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.types import (
    EVENT_SP_ROLLBACK_FAILED,
    EVENT_SP_ROLLBACK_STARTED,
    OUTBOX_TOPIC_HANDLER_ROLLBACK,
    ROLLBACK_STATUS_COMPLETED,
    ROLLBACK_STATUS_FAILED,
    ROLLBACK_STATUS_RUNNING,
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
        name="rollback-test", models=list(ALL_MODELS), config=config, backend_class=SQLiteBackend,
    ) as group:
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        yield group
        drop_tables(backend)


@pytest.fixture
def reversible_instance(backend_group):
    """A 3-step instance where the inventory subprocess is marked reversible."""
    template, steps = make_steps()
    template.save()
    for step in steps:
        step.save()

    instance = SyncOrderFactory().create(template, steps, context={"user_id": "u1"})
    inventory = instance.get_subprocess("inventory")
    inventory.is_reversible = True  # runtime flag, not a template property

    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()
    return instance


def _advance_to_failed(service, instance, subprocess):
    """Drive a subprocess into a rollback-eligible 'failed' state."""
    service.publish_event(
        order_id=instance.order.id,
        subprocess_id=subprocess.id,
        new_status="failed",
        event_key=f"fail-{subprocess.step_name}",
    )


def test_can_rollback_requires_reversible_flag(backend_group, reversible_instance):
    """A subprocess without is_reversible cannot roll back even from a rollback state."""
    service = SyncOrderService()
    payment = reversible_instance.get_subprocess("payment")
    payment.is_reversible = False
    payment.save()
    _advance_to_failed(service, reversible_instance, payment)

    with pytest.raises(InvalidStateTransitionError):
        service.publish_rollback(
            order_id=reversible_instance.order.id, subprocess_id=payment.id,
        )


def test_can_rollback_requires_rollback_state(backend_group, reversible_instance):
    """A subprocess in a non-rollback terminal state (e.g. 'locked') cannot roll back."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=reversible_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-lock-1",
    )

    # 'locked' is an advance state, not a rollback state.
    with pytest.raises(InvalidStateTransitionError):
        service.publish_rollback(
            order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        )


def test_publish_rollback_marks_running_and_enqueues_outbox(backend_group, reversible_instance):
    """publish_rollback flips rollback_status to running and writes a handler_rollback outbox item."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    result = service.publish_rollback(
        order_id=reversible_instance.order.id,
        subprocess_id=inventory.id,
        event_key="rollback-start-1",
    )

    assert result.event.event_type == EVENT_SP_ROLLBACK_STARTED
    assert len(result.outbox_items) == 1
    assert result.outbox_items[0].topic == OUTBOX_TOPIC_HANDLER_ROLLBACK

    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_RUNNING
    assert reloaded.rollback_started_at is not None


def test_handler_rollback_topic_completes_lifecycle(backend_group, reversible_instance):
    """The handler_rollback topic calls handler.rollback() and marks rollback completed."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    class RollbackHandler(SyncSubProcessHandler):
        def start(self):
            return HandlerResult(status="locked")

        def rollback(self):
            return HandlerResult(status="rolled_back", event_key="rollback-done-1")

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", RollbackHandler)
    topic = SyncHandlerRollbackTopic(registry, service)

    start_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
    )
    ok = topic(start_result.outbox_items[0])

    assert ok is True
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_COMPLETED
    assert reloaded.rollback_completed_at is not None


def test_handler_rollback_topic_records_failure_on_exception(backend_group, reversible_instance):
    """If handler.rollback() raises, the topic marks rollback failed and records the error."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    class BoomHandler(SyncSubProcessHandler):
        def start(self):
            return HandlerResult(status="locked")

        def rollback(self):
            raise RuntimeError("compensation failed")

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", BoomHandler)
    topic = SyncHandlerRollbackTopic(registry, service)

    start_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
    )
    ok = topic(start_result.outbox_items[0])

    assert ok is True  # the deliverer treats this as delivered (error was recorded)
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_FAILED
    assert reloaded.rollback_error is not None
    assert "compensation failed" in reloaded.rollback_error["error"]

    # A rollback-failed event should be in the audit trail.
    events = OrderEvent.query().where(OrderEvent.c.subprocess_id == inventory.id).all()
    assert any(e.event_type == EVENT_SP_ROLLBACK_FAILED for e in events)


def test_publish_rollback_is_idempotent(backend_group, reversible_instance):
    """Reissuing publish_rollback with the same event_key returns duplicate=True."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    first = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        event_key="rollback-idem-1",
    )
    second = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        event_key="rollback-idem-1",
    )

    assert second.duplicate is True
    assert second.event.id == first.event.id

    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    # rollback_status stays running (only one transition recorded)
    assert reloaded.rollback_status == ROLLBACK_STATUS_RUNNING


def test_handler_rollback_topic_skips_publish_when_handler_returns_none(
    backend_group, reversible_instance
):
    """A handler returning None from rollback() still completes the rollback lifecycle."""

    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    class NoOpRollbackHandler(SyncSubProcessHandler):
        def start(self):
            return HandlerResult(status="locked")

        def rollback(self):
            return None  # no status to publish

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", NoOpRollbackHandler)
    topic = SyncHandlerRollbackTopic(registry, service)

    start_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
    )
    ok = topic(start_result.outbox_items[0])

    assert ok is True
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_COMPLETED
    assert reloaded.rollback_completed_at is not None


def test_rollback_cannot_be_initiated_twice(backend_group, reversible_instance):
    """Once rollback_status is 'running', a second publish_rollback is rejected."""
    service = SyncOrderService()
    inventory = reversible_instance.get_subprocess("inventory")
    _advance_to_failed(service, reversible_instance, inventory)

    service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        event_key="rollback-once-1",
    )

    with pytest.raises(InvalidStateTransitionError):
        service.publish_rollback(
            order_id=reversible_instance.order.id, subprocess_id=inventory.id,
            event_key="rollback-once-2",
        )
