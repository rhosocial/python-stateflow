import uuid

from rhosocial.stateflow import Order, OrderSubProcess
from rhosocial.stateflow.types import ORDER_STATUS_COMPLETED


def make_subprocess(status, skipped=False):
    return OrderSubProcess(
        process_id=uuid.uuid4(),
        step_name="step",
        handler_class="tests.Handler",
        terminal_states=["done"],
        advance_states=["done"],
        status=status,
        skipped=skipped,
    )


def test_mark_completed_sets_status_and_timestamp():
    order = Order(template_id=uuid.uuid4())

    order.mark_completed()

    assert order.status == ORDER_STATUS_COMPLETED
    assert order.completed_at is not None


def test_all_subprocesses_completed_ignores_skipped_subprocesses():
    order = Order(template_id=uuid.uuid4())

    assert order.all_subprocesses_completed([
        make_subprocess("done"),
        make_subprocess("stateflow:subprocess:pending", skipped=True),
    ])


def test_all_subprocesses_completed_requires_active_advance_state():
    order = Order(template_id=uuid.uuid4())

    assert not order.all_subprocesses_completed([make_subprocess("stateflow:subprocess:pending")])
