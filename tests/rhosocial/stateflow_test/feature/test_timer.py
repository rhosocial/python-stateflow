# tests/rhosocial/stateflow_test/feature/test_timer.py
"""Timeout scheduler tests: mark_running sets timeout_at, tick publishes timeouts."""

from datetime import datetime, timedelta, timezone

import pytest

from rhosocial.stateflow import (
    FlowPath,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
    SyncOrderFactory,
    SyncOrderService,
    SyncTimeoutScheduler,
)
from rhosocial.stateflow.types import EVENT_SP_STATUS_CHANGED

from .helpers import make_steps

ALL_MODELS = (
    OrderTemplate, OrderTemplateStep, FlowPath, Order, OrderProcess,
    OrderSubProcess, SubProcessDependency, OrderEvent, OrderOutbox,
)





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


def test_mark_running_sets_timeout_at_when_configured(backend_group, persisted_instance):
    """A subprocess with timeout_seconds gets timeout_at = started_at + timeout_seconds."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    # Advance inventory → locked to start payment (which has timeout_seconds=300)
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-1",
    )
    payment = persisted_instance.get_subprocess("payment")
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert reloaded.status == "running"
    assert reloaded.started_at is not None
    assert reloaded.timeout_at is not None
    expected = reloaded.started_at + timedelta(seconds=300)
    # Allow a small clock skew tolerance.
    assert abs((reloaded.timeout_at - expected).total_seconds()) < 1


def test_mark_running_skips_timeout_when_not_configured(backend_group, persisted_instance):
    """A subprocess without timeout_seconds leaves timeout_at as None."""
    inventory = persisted_instance.get_subprocess("inventory")
    # inventory has no timeout_seconds configured.
    # Mark it running directly (without going through the full dispatch pipeline).
    inventory.mark_running()
    inventory.save()
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.timeout_at is None


def test_tick_publishes_timeout_for_due_subprocess(backend_group, persisted_instance):
    """tick() picks up due subprocesses and publishes their timeout_status."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-1",
    )
    payment = persisted_instance.get_subprocess("payment")
    # Manually expire the timeout by updating the model directly.
    payment_db = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    payment_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    payment_db.save()

    scheduler = SyncTimeoutScheduler(service)
    processed = scheduler.tick()

    assert processed == 1
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert reloaded.status == "timeout"
    assert reloaded.completed_at is not None

    # A status-changed event recording the timeout transition must be in the audit trail.
    events = OrderEvent.query().where(OrderEvent.c.subprocess_id == payment.id).all()
    assert any(
        e.event_type == EVENT_SP_STATUS_CHANGED and e.to_status == "timeout" for e in events
    )


def test_tick_skips_subprocesses_not_yet_due(backend_group, persisted_instance):
    """tick() with a future `now` only sees subprocesses whose timeout_at has elapsed."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-1",
    )
    payment = OrderSubProcess.query().where(
        OrderSubProcess.c.id == persisted_instance.get_subprocess("payment").id
    ).one()
    assert payment.timeout_at is not None  # set by mark_running

    # `now` before the timeout_at → nothing to process.
    scheduler = SyncTimeoutScheduler(service)
    processed = scheduler.tick(now=payment.timeout_at - timedelta(seconds=1))
    assert processed == 0
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert reloaded.status == "running"


def test_tick_respects_limit(backend_group, persisted_instance):
    """limit caps how many timeouts one tick processes."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-1",
    )
    payment = persisted_instance.get_subprocess("payment")
    payment_db = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    payment_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    payment_db.save()

    scheduler = SyncTimeoutScheduler(service)
    processed = scheduler.tick(limit=0)
    assert processed == 0


def test_publish_timeout_is_idempotent_with_event_key(backend_group, persisted_instance):
    """Re-applying the same timeout event_key returns duplicate=True."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-1",
    )
    payment = persisted_instance.get_subprocess("payment")
    payment_db = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    payment_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    payment_db.save()

    first = service.publish_timeout(
        order_id=persisted_instance.order.id,
        subprocess_id=payment.id,
        event_key="timeout:payment-1",
    )
    assert first.duplicate is False
    assert first.event.to_status == "timeout"

    second = service.publish_timeout(
        order_id=persisted_instance.order.id,
        subprocess_id=payment.id,
        event_key="timeout:payment-1",
    )
    assert second.duplicate is True
    assert second.event.id == first.event.id


def test_tick_retries_failed_timeout_with_backoff(backend_group, persisted_instance):
    """A failed timeout is rescheduled (timeout_at pushed into the future) so the next sweep retries."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-retry-1",
    )
    payment = persisted_instance.get_subprocess("payment")
    payment_db = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    payment_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    payment_db.save()

    def boom(*args, **kwargs):
        raise RuntimeError("simulated timeout failure")

    scheduler = SyncTimeoutScheduler(service, retry_base_delay=60.0)
    original = service.publish_timeout
    service.publish_timeout = boom  # monkeypatch instance method
    try:
        processed = scheduler.tick()
    finally:
        service.publish_timeout = original
    assert processed == 0

    # The subprocess must be rescheduled into the future (backoff), not hot-looped.
    rescheduled = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert rescheduled.timeout_at is not None
    assert rescheduled.timeout_at > datetime.now(timezone.utc)
    assert rescheduled.extra.get("timeout_retry_count") == 1

    # A second failure reschedules with a longer delay (exponential backoff).
    rescheduled.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    rescheduled.save()
    service.publish_timeout = boom
    try:
        scheduler.tick()
    finally:
        service.publish_timeout = original
    rescheduled2 = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert rescheduled2.extra.get("timeout_retry_count") == 2
    assert rescheduled2.timeout_at > rescheduled.timeout_at

