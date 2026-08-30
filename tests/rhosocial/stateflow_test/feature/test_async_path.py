# tests/rhosocial/stateflow_test/feature/test_async_path.py
"""End-to-end async path tests: AsyncSQLiteBackend + async models + native await.

These tests verify the **genuine async** implementation — no
``asyncio.to_thread``, no sync model references. Every DB operation is a
coroutine provided by ``AsyncActiveRecord`` + ``AsyncSQLiteBackend``.
"""

import uuid

import pytest

from rhosocial.stateflow import (
    AsyncHandlerStartTopic,
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderFactory,
    AsyncOrderOutbox,
    AsyncOrderOutboxDeliverer,
    AsyncOrderProcess,
    AsyncOrderService,
    AsyncOrderSubProcess,
    AsyncOrderTemplate,
    AsyncOrderTemplateStep,
    AsyncSubProcessDependency,
    AsyncTimeoutScheduler,
    AsyncSubProcessHandler,
    FlowPath,
    HandlerRegistry,
    HandlerResult,
)
from rhosocial.stateflow.types import (
    EVENT_SP_ROLLBACK_STARTED,
    EVENT_SP_STATUS_CHANGED,
    OUTBOX_STATUS_SENT,
    OUTBOX_TOPIC_HANDLER_START,
)


ALL_ASYNC_MODELS = (
    AsyncOrderTemplate,
    AsyncOrderTemplateStep,
    FlowPath,  # no async sibling needed — not used in async path
    AsyncOrder,
    AsyncOrderProcess,
    AsyncOrderSubProcess,
    AsyncSubProcessDependency,
    AsyncOrderEvent,
    AsyncOrderOutbox,
)





@pytest.fixture
async def async_persisted_instance(async_backend_group):
    """Build and persist a 3-step order via AsyncOrderFactory with async saves."""
    # Use async model classes throughout — no sync model references.
    template = AsyncOrderTemplate(name="purchase", version=1)
    await template.save()

    inventory = AsyncOrderTemplateStep(
        template_id=template.id,
        name="inventory",
        handler_class="tests.InventoryHandler",
        terminal_states=["locked", "failed"],
        advance_states=["locked"],
        rollback_states=["failed"],
        step_order=1,
    )
    payment = AsyncOrderTemplateStep(
        template_id=template.id,
        name="payment",
        handler_class="tests.PaymentHandler",
        terminal_states=["paid", "payment_failed", "timeout"],
        advance_states=["paid"],
        rollback_states=["payment_failed"],
        timeout_seconds=300,
        timeout_status="timeout",
        depends_on=["inventory"],
        step_order=2,
    )
    shipment = AsyncOrderTemplateStep(
        template_id=template.id,
        name="shipment",
        handler_class="tests.ShipmentHandler",
        terminal_states=["shipped", "failed"],
        advance_states=["shipped"],
        rollback_states=["failed"],
        depends_on=["payment"],
        step_order=3,
    )
    for step in (inventory, payment, shipment):
        await step.save()

    instance = await AsyncOrderFactory().create(template, [inventory, payment, shipment], context={"user_id": "u1"})
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()
    for d in instance.dependencies:
        await d.save()
    for e in instance.events:
        await e.save()
    return instance


@pytest.mark.asyncio
async def test_async_service_publish_event_persists(async_backend_group, async_persisted_instance):
    """AsyncOrderService.publish_event writes the new status and event via native await."""
    service = AsyncOrderService()
    inventory = async_persisted_instance.get_subprocess("inventory")

    result = await service.publish_event(
        order_id=async_persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-async-1",
    )

    assert result.duplicate is False
    assert result.event.event_type == EVENT_SP_STATUS_CHANGED
    assert result.event.to_status == "locked"

    reloaded = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == inventory.id
    ).one()
    assert reloaded.status == "locked"
    assert reloaded.completed_at is not None


@pytest.mark.asyncio
async def test_async_service_starts_downstream(async_backend_group, async_persisted_instance):
    """An advance state in the async path must start downstream subprocesses."""
    service = AsyncOrderService()
    inventory = async_persisted_instance.get_subprocess("inventory")

    result = await service.publish_event(
        order_id=async_persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
    )

    assert [sp.step_name for sp in result.started_subprocesses] == ["payment"]
    assert len(result.outbox_items) == 1
    assert result.outbox_items[0].topic == OUTBOX_TOPIC_HANDLER_START

    payment = async_persisted_instance.get_subprocess("payment")
    reloaded = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == payment.id
    ).one()
    assert reloaded.status == "stateflow:subprocess:running"


@pytest.mark.asyncio
async def test_async_service_idempotent(async_backend_group, async_persisted_instance):
    """Replaying the same event_key returns duplicate=True and writes nothing new."""
    service = AsyncOrderService()
    inventory = async_persisted_instance.get_subprocess("inventory")
    order_id = async_persisted_instance.order.id

    await service.publish_event(
        order_id=order_id, subprocess_id=inventory.id,
        new_status="locked", event_key="inv-idem-1",
    )
    events_before = await AsyncOrderEvent.query().where(
        AsyncOrderEvent.c.order_id == order_id
    ).all()
    count_before = len(events_before)

    second = await service.publish_event(
        order_id=order_id, subprocess_id=inventory.id,
        new_status="locked", event_key="inv-idem-1",
    )

    assert second.duplicate is True
    events_after = await AsyncOrderEvent.query().where(
        AsyncOrderEvent.c.order_id == order_id
    ).all()
    assert len(events_after) == count_before


@pytest.mark.asyncio
async def test_async_service_completes_order(async_backend_group, async_persisted_instance):
    """Advancing every subprocess completes the order in the async path."""
    service = AsyncOrderService()
    order_id = async_persisted_instance.order.id

    inv = async_persisted_instance.get_subprocess("inventory")
    await service.publish_event(order_id=order_id, subprocess_id=inv.id, new_status="locked")

    pay = async_persisted_instance.get_subprocess("payment")
    await service.publish_event(order_id=order_id, subprocess_id=pay.id, new_status="paid")

    shp = async_persisted_instance.get_subprocess("shipment")
    await service.publish_event(order_id=order_id, subprocess_id=shp.id, new_status="shipped")

    reloaded = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
    assert reloaded.status == "stateflow:order:completed"
    assert reloaded.completed_at is not None


@pytest.mark.asyncio
async def test_async_deliverer_marks_sent(async_backend_group, async_persisted_instance):
    """AsyncOrderOutboxDeliverer drains pending items via native await."""
    # Seed an outbox item directly
    item = AsyncOrderOutbox(
        event_id=uuid.uuid4(),
        topic=OUTBOX_TOPIC_HANDLER_START,
        payload={"subprocess_id": str(async_persisted_instance.get_subprocess("inventory").id)},
    )
    await item.save()

    deliverer = AsyncOrderOutboxDeliverer()

    async def ok_handler(item):
        return True

    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, ok_handler)

    processed = await deliverer.deliver_pending()

    assert processed == 1
    sent = await AsyncOrderOutbox.query().where(
        AsyncOrderOutbox.c.status == OUTBOX_STATUS_SENT
    ).all()
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_async_handler_start_topic(async_backend_group, async_persisted_instance):
    """AsyncHandlerStartTopic resolves the handler, awaits start(), and publishes the result."""

    class FakeHandler(AsyncSubProcessHandler):
        async def start(self):
            return HandlerResult(status="locked", event_key="async-start-1")
        async def rollback(self):
            return None

    registry = HandlerRegistry()
    registry.register("tests.InventoryHandler", FakeHandler)
    service = AsyncOrderService()
    topic = AsyncHandlerStartTopic(registry, service)

    inventory = async_persisted_instance.get_subprocess("inventory")
    outbox_item = AsyncOrderOutbox(
        event_id=uuid.uuid4(),
        topic=OUTBOX_TOPIC_HANDLER_START,
        payload={"subprocess_id": str(inventory.id)},
    )
    await outbox_item.save()

    ok = await topic(outbox_item)

    assert ok is True
    reloaded = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == inventory.id
    ).one()
    assert reloaded.status == "locked"


@pytest.mark.asyncio
async def test_async_timeout_scheduler(async_backend_group, async_persisted_instance):
    """AsyncTimeoutScheduler.tick publishes the timeout_status for due subprocesses."""
    from datetime import datetime, timedelta, timezone

    service = AsyncOrderService()
    inventory = async_persisted_instance.get_subprocess("inventory")
    # Advance inventory → locked to start payment (which has timeout_seconds=300)
    await service.publish_event(
        order_id=async_persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="locked",
        event_key="inv-timer-1",
    )
    payment = async_persisted_instance.get_subprocess("payment")

    # Expire the timeout by loading the subprocess, setting a past timeout_at, and saving.
    payment_db = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == payment.id
    ).one()
    payment_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    await payment_db.save()

    scheduler = AsyncTimeoutScheduler(service)
    processed = await scheduler.tick()

    assert processed == 1
    reloaded = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == payment.id
    ).one()
    assert reloaded.status == "timeout"


@pytest.mark.asyncio
async def test_async_publish_rollback(async_backend_group, async_persisted_instance):
    """AsyncOrderService.publish_rollback begins the rollback lifecycle."""
    from rhosocial.stateflow.types import ROLLBACK_STATUS_RUNNING

    # Make inventory reversible before saving
    inventory = async_persisted_instance.get_subprocess("inventory")
    inventory.is_reversible = True
    await inventory.save()

    service = AsyncOrderService()
    # First drive inventory into a rollback-eligible state ("failed")
    await service.publish_event(
        order_id=async_persisted_instance.order.id,
        subprocess_id=inventory.id,
        new_status="failed",
        event_key="inv-fail-1",
    )

    result = await service.publish_rollback(
        order_id=async_persisted_instance.order.id,
        subprocess_id=inventory.id,
        event_key="inv-rollback-1",
    )

    assert result.event.event_type == EVENT_SP_ROLLBACK_STARTED
    reloaded = await AsyncOrderSubProcess.query().where(
        AsyncOrderSubProcess.c.id == inventory.id
    ).one()
    assert reloaded.rollback_status == ROLLBACK_STATUS_RUNNING
    assert reloaded.rollback_started_at is not None
