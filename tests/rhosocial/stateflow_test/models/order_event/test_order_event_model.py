import uuid

from rhosocial.stateflow import Order, OrderEvent, OrderSubProcess
from rhosocial.stateflow.types import (
    EVENT_CONFLICT,
    EVENT_ORDER_COMPLETED,
    EVENT_ORDER_CREATED,
    EVENT_SP_CREATED,
    EVENT_SP_SKIPPED,
    EVENT_SP_STATUS_CHANGED,
)


def make_order():
    return Order(template_id=uuid.uuid4())


def make_subprocess(skipped=False):
    return OrderSubProcess(
        process_id=uuid.uuid4(),
        step_name="inventory",
        handler_class="tests.Handler",
        terminal_states=["done"],
        advance_states=["done"],
        skipped=skipped,
    )


def test_order_created_builds_order_event():
    order = make_order()

    event = OrderEvent.order_created(order)

    assert event.order_id == order.id
    assert event.event_type == EVENT_ORDER_CREATED


def test_subprocess_created_and_skipped_build_step_events():
    order = make_order()
    subprocess = make_subprocess()
    skipped = make_subprocess(skipped=True)

    created = OrderEvent.subprocess_created(order, subprocess)
    skipped_event = OrderEvent.subprocess_skipped(order, skipped)

    assert created.event_type == EVENT_SP_CREATED
    assert created.payload == {"step_name": "inventory"}
    assert skipped_event.event_type == EVENT_SP_SKIPPED


def test_status_changed_and_conflict_events_capture_transition():
    order = make_order()
    subprocess = make_subprocess()

    changed = OrderEvent.status_changed(order, subprocess, "stateflow:subprocess:pending", "done", {"x": 1}, "k1")
    conflict = OrderEvent.conflict_event(order, subprocess, "failed", event_key="k2")

    assert changed.event_type == EVENT_SP_STATUS_CHANGED
    assert changed.from_status == "stateflow:subprocess:pending"
    assert changed.to_status == "done"
    assert changed.payload == {"x": 1}
    assert changed.event_key == "k1"
    assert conflict.event_type == EVENT_CONFLICT
    assert conflict.conflict
    assert conflict.event_key == "k2"


def test_order_completed_builds_completion_event():
    order = make_order()

    event = OrderEvent.order_completed(order)

    assert event.event_type == EVENT_ORDER_COMPLETED
    assert event.order_id == order.id


def test_find_by_event_key_returns_matching_event():
    matching = OrderEvent(order_id=uuid.uuid4(), event_type="custom", event_key="k1")
    other = OrderEvent(order_id=uuid.uuid4(), event_type="custom", event_key="k2")

    assert OrderEvent.find_by_event_key([other, matching], "k1") is matching
    assert OrderEvent.find_by_event_key([other, matching], "missing") is None
    assert OrderEvent.find_by_event_key([other, matching], None) is None
