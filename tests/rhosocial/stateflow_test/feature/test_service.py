# tests/rhosocial/stateflow_test/feature/test_service.py
"""Transactional service tests: load → dispatch → persist in one transaction."""

import uuid

import pytest

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    ConcurrentStateTransitionError,
    InvalidStateTransitionError,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    FlowPath,
    SubProcessDependency,
    SyncOrderFactory,
    SyncOrderService,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.service import _CONCURRENCY_MESSAGE
from rhosocial.stateflow.types import (
    EVENT_CONFLICT,
    EVENT_SP_STATUS_CHANGED,
    OUTBOX_STATUS_PENDING,
    OUTBOX_TOPIC_HANDLER_START,
    SUBPROCESS_STATUS_PENDING,
    SUBPROCESS_STATUS_RUNNING,
)

from .helpers import make_steps

ALL_MODELS = (
    OrderTemplate,
    OrderTemplateStep,
    FlowPath,
    Order,
    OrderProcess,
    OrderSubProcess,
    SubProcessDependency,
    OrderEvent,
    OrderOutbox,
)


@pytest.fixture
def backend_group():
    """Configure all stateflow models on a single in-memory SQLite backend."""
    config = SQLiteConnectionConfig(database=":memory:")
    with BackendGroup(
        name="stateflow-test",
        models=list(ALL_MODELS),
        config=config,
        backend_class=SQLiteBackend,
    ) as group:
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        yield group
        drop_tables(backend)


@pytest.fixture
def persisted_instance(backend_group):
    """Build and persist a 3-step purchase order (inventory → payment → shipment)."""
    template, steps = make_steps()
    template.save()
    for step in steps:
        step.save()

    instance = SyncOrderFactory().create(template, steps, context={"user_id": "u123"})
    instance.order.save()
    instance.process.save()
    for subprocess in instance.subprocesses:
        subprocess.save()
    for dependency in instance.dependencies:
        dependency.save()
    for event in instance.events:
        event.save()
    return instance


def test_publish_event_persists_status_and_event(backend_group, persisted_instance):
    """publish_event writes the new subprocess status and the status-changed event."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")

    result = service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inventory-1",
    )

    assert result.duplicate is False
    assert result.event.event_type == EVENT_SP_STATUS_CHANGED
    assert result.event.from_status == SUBPROCESS_STATUS_PENDING
    assert result.event.to_status == "locked"

    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.status == "locked"
    assert reloaded.completed_at is not None

    events = OrderEvent.query().where(OrderEvent.c.order_id == persisted_instance.order.id).all()
    assert any(e.event_type == EVENT_SP_STATUS_CHANGED and e.to_status == "locked" for e in events)


def test_publish_event_starts_downstream_and_writes_outbox(backend_group, persisted_instance):
    """An advance state must start downstream subprocesses and enqueue handler_start outbox."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")

    result = service.publish_event(
        order_id=persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
    )

    assert [sp.step_name for sp in result.started_subprocesses] == ["payment"]
    assert len(result.outbox_items) == 1
    assert result.outbox_items[0].topic == OUTBOX_TOPIC_HANDLER_START

    payment = persisted_instance.get_subprocess("payment")
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert reloaded.status == SUBPROCESS_STATUS_RUNNING
    assert reloaded.started_at is not None

    outbox = OrderOutbox.query().where(OrderOutbox.c.event_id == result.event.id).all()
    assert len(outbox) == 1
    assert outbox[0].status == OUTBOX_STATUS_PENDING


def test_publish_event_is_idempotent_for_repeated_event_key(backend_group, persisted_instance):
    """Replaying the same event_key returns duplicate=True and writes nothing new."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    order_id = persisted_instance.order.id

    first = service.publish_event(
        order_id=order_id, subprocess_id=inventory.id, new_status="locked", event_key="inventory-1",
    )
    events_before = OrderEvent.query().where(OrderEvent.c.order_id == order_id).count()

    second = service.publish_event(
        order_id=order_id, subprocess_id=inventory.id, new_status="locked", event_key="inventory-1",
    )

    assert second.duplicate is True
    assert second.event.id == first.event.id
    events_after = OrderEvent.query().where(OrderEvent.c.order_id == order_id).count()
    assert events_after == events_before


def test_publish_event_marks_order_completed_when_all_advance(backend_group, persisted_instance):
    """Advancing every subprocess to its advance state completes the order."""
    service = SyncOrderService()
    order_id = persisted_instance.order.id

    inv = persisted_instance.get_subprocess("inventory")
    service.publish_event(order_id=order_id, subprocess_id=inv.id, new_status="locked")

    pay = persisted_instance.get_subprocess("payment")
    service.publish_event(order_id=order_id, subprocess_id=pay.id, new_status="paid")

    shp = persisted_instance.get_subprocess("shipment")
    service.publish_event(order_id=order_id, subprocess_id=shp.id, new_status="shipped")

    reloaded = Order.query().where(Order.c.id == order_id).one()
    assert reloaded.status == "completed"
    assert reloaded.completed_at is not None


def test_publish_event_terminal_overwrite_yields_conflict(backend_group, persisted_instance):
    """Once a subprocess reaches a terminal state, a divergent status becomes a conflict event."""
    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")
    order_id = persisted_instance.order.id

    service.publish_event(order_id=order_id, subprocess_id=inventory.id, new_status="locked")

    conflict_result = service.publish_event(
        order_id=order_id, subprocess_id=inventory.id, new_status="failed",
    )

    assert conflict_result.event.event_type == EVENT_CONFLICT
    assert conflict_result.event.conflict is True

    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == inventory.id).one()
    assert reloaded.status == "locked"


def test_publish_event_skipped_subprocess_rejected(backend_group, persisted_instance, monkeypatch):
    """A skipped subprocess cannot receive events via the service either."""
    # Rebuild the instance skipping the payment step.
    template, steps = make_steps()
    template.save()
    for step in steps:
        step.save()
    instance = SyncOrderFactory().create(template, steps, skip_steps=["payment"])
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()

    service = SyncOrderService()
    payment = instance.get_subprocess("payment")

    with pytest.raises(InvalidStateTransitionError):
        service.publish_event(
            order_id=instance.order.id, subprocess_id=payment.id, new_status="paid",
        )


def test_publish_event_concurrent_transition_raises_domain_error(
    backend_group, persisted_instance, monkeypatch
):
    """A stale version (optimistic lock failure) surfaces as ConcurrentStateTransitionError.

    The service loads the subprocess fresh from the database, so simply bumping
    the version ahead of the call would be reloaded and silently succeed. To
    exercise the optimistic-lock branch we monkeypatch ``save`` to raise the
    exact ``DatabaseError`` that ``OptimisticLockMixin`` emits when the
    UPDATE affects zero rows.
    """
    from rhosocial.activerecord.backend.errors import DatabaseError

    service = SyncOrderService()
    inventory = persisted_instance.get_subprocess("inventory")

    def raise_concurrency(self):
        raise DatabaseError(_CONCURRENCY_MESSAGE)

    monkeypatch.setattr(OrderSubProcess, "save", raise_concurrency)

    with pytest.raises(ConcurrentStateTransitionError):
        service.publish_event(
            order_id=persisted_instance.order.id,
            subprocess_id=inventory.id,
            new_status="locked",
        )


def test_publish_timeout_applies_timeout_status(backend_group):
    """publish_timeout drives a subprocess into its configured timeout_status."""
    template, steps = make_steps()
    template.save()
    for step in steps:
        step.save()
    instance = SyncOrderFactory().create(template, steps)
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()

    service = SyncOrderService()
    payment = instance.get_subprocess("payment")
    payment.mark_running()
    payment.save()

    result = service.publish_timeout(
        order_id=instance.order.id, subprocess_id=payment.id,
    )

    assert result.event.to_status == "timeout"
    reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == payment.id).one()
    assert reloaded.status == "timeout"
    assert reloaded.completed_at is not None


def test_publish_event_unknown_order_raises(backend_group):
    """Missing order surfaces as a ValueError before any dispatch work happens."""
    service = SyncOrderService()
    with pytest.raises(ValueError, match="Order"):
        service.publish_event(
            order_id=uuid.uuid4(), subprocess_id=uuid.uuid4(), new_status="locked",
        )
