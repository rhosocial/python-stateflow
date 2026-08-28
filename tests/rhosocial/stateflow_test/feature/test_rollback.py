# tests/rhosocial/stateflow_test/feature/test_rollback.py
"""Rollback lifecycle tests: begin → handler.rollback → complete/fail."""


import pytest

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
    """If handler.rollback() raises, the topic retries via outbox until max_rollback_retries, then marks failed."""
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
    topic = SyncHandlerRollbackTopic(registry, service, max_rollback_retries=3)

    start_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
    )
    outbox_item = start_result.outbox_items[0]

    # First two attempts: retryable (returns False). The outbox deliverer would
    # increment retry_count between deliveries; simulate that here.
    for attempt in range(1, 3):
        outbox_item.retry_count = attempt - 1
        ok = topic(outbox_item)
        assert ok is False, f"attempt {attempt} should be retryable"
        reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
        assert reloaded.rollback_status == ROLLBACK_STATUS_RUNNING, f"attempt {attempt} should keep running"
        assert reloaded.rollback_error is not None

    # Third attempt: retry_count=2 → 2+1 >= 3 → permanent failure
    outbox_item.retry_count = 2
    ok = topic(outbox_item)
    assert ok is True  # the deliverer treats this as handled (error was recorded permanently)
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


def test_rollback_can_be_retried_after_failure(backend_group, reversible_instance):
    """After a permanent rollback failure, publish_rollback can be retried.

    ``can_rollback`` allows ``rollback_status == failed``, so an operator can
    re-initiate the rollback (e.g. after fixing the root cause).
    """
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
    # max_rollback_retries=1 → the first failure is permanent.
    topic = SyncHandlerRollbackTopic(registry, service, max_rollback_retries=1)

    start_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        event_key="rb-retry-1",
    )
    topic(start_result.outbox_items[0])
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_FAILED
    assert reloaded.can_rollback()  # failed allows retry

    # Re-initiate rollback — should be allowed now.
    retry_result = service.publish_rollback(
        order_id=reversible_instance.order.id, subprocess_id=inventory.id,
        event_key="rb-retry-2",
    )
    assert retry_result.duplicate is False
    reloaded2 = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded2.rollback_status == ROLLBACK_STATUS_RUNNING
    assert reloaded2.rollback_error is None  # cleared on retry begin
