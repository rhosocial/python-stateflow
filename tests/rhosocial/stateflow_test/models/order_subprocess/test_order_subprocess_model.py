import uuid

from rhosocial.stateflow import OrderProcess, OrderSubProcess, OrderTemplateStep
from rhosocial.stateflow.types import SUBPROCESS_SOURCE_DYNAMIC, SUBPROCESS_SOURCE_TEMPLATE, SUBPROCESS_STATUS_RUNNING


def make_step():
    return OrderTemplateStep(
        template_id=uuid.uuid4(),
        name="inventory",
        handler_class="tests.Handler",
        terminal_states=["done", "failed"],
        advance_states=["done"],
        rollback_states=["failed"],
        timeout_seconds=30,
        timeout_status="failed",
    )


def test_from_template_step_copies_step_configuration():
    step = make_step()
    process_id = uuid.uuid4()

    subprocess = OrderSubProcess.from_template_step(process_id, step, False, 3)

    assert subprocess.process_id == process_id
    assert subprocess.step_name == "inventory"
    assert subprocess.source == SUBPROCESS_SOURCE_TEMPLATE
    assert subprocess.sequence == 3
    assert subprocess.advance_states == ["done"]


def test_dynamic_builds_appended_subprocess_with_next_sequence():
    process = OrderProcess(order_id=uuid.uuid4())
    existing = OrderSubProcess.from_template_step(process.id, make_step(), False, 4)

    subprocess = OrderSubProcess.dynamic(
        process,
        [existing],
        name="payment",
        handler_class="tests.PaymentHandler",
        terminal_states=["paid"],
        advance_states=["paid"],
    )

    assert subprocess.source == SUBPROCESS_SOURCE_DYNAMIC
    assert subprocess.sequence == 5
    assert subprocess.step_name == "payment"


def test_status_helpers_and_transitions():
    subprocess = OrderSubProcess.from_template_step(uuid.uuid4(), make_step(), False, 0)

    previous = subprocess.apply_status("done")

    assert previous == "pending"
    assert subprocess.is_terminal()
    assert subprocess.is_advance_status()
    assert subprocess.completed_at is not None


def test_mark_running_sets_running_status_and_timestamp():
    subprocess = OrderSubProcess.from_template_step(uuid.uuid4(), make_step(), False, 0)

    subprocess.mark_running()

    assert subprocess.status == SUBPROCESS_STATUS_RUNNING
    assert subprocess.started_at is not None


def test_dependency_satisfied_accepts_skipped_or_advance_status():
    skipped = OrderSubProcess.from_template_step(uuid.uuid4(), make_step(), True, 0)
    completed = OrderSubProcess.from_template_step(uuid.uuid4(), make_step(), False, 0)
    pending = OrderSubProcess.from_template_step(uuid.uuid4(), make_step(), False, 0)
    completed.status = "done"

    assert skipped.dependency_satisfied()
    assert completed.dependency_satisfied()
    assert not pending.dependency_satisfied()
