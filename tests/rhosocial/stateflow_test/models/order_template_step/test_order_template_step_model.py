import uuid

from rhosocial.stateflow import OrderTemplateStep


def test_state_sets_are_returned_as_sets():
    step = OrderTemplateStep(
        template_id=uuid.uuid4(),
        name="payment",
        handler_class="tests.Handler",
        terminal_states=["paid", "failed"],
        advance_states=["paid"],
        rollback_states=["failed"],
    )

    assert step.terminal_state_set() == {"paid", "failed"}
    assert step.advance_state_set() == {"paid"}
    assert step.rollback_state_set() == {"failed"}


def test_missing_state_helpers_report_non_terminal_states():
    step = OrderTemplateStep(
        template_id=uuid.uuid4(),
        name="payment",
        handler_class="tests.Handler",
        terminal_states=["paid"],
        advance_states=["paid", "manual_review"],
        rollback_states=["failed"],
    )

    assert step.missing_advance_states() == {"manual_review"}
    assert step.missing_rollback_states() == {"failed"}


def test_timeout_helpers_validate_required_terminal_status():
    missing = OrderTemplateStep(
        template_id=uuid.uuid4(),
        name="payment",
        handler_class="tests.Handler",
        terminal_states=["paid"],
        timeout_seconds=30,
    )
    invalid = OrderTemplateStep(
        template_id=uuid.uuid4(),
        name="shipment",
        handler_class="tests.Handler",
        terminal_states=["shipped"],
        timeout_seconds=30,
        timeout_status="expired",
    )

    assert missing.requires_timeout_status()
    assert not invalid.has_terminal_timeout_status()
