# tests/rhosocial/stateflow_test/feature/test_template_validator.py

from rhosocial.stateflow import FlowPath, OrderTemplateStep, OrderTemplateValidator

from .helpers import make_steps, make_template


def test_valid_dag_passes():
    _, steps = make_steps()

    result = OrderTemplateValidator().validate(steps)

    assert result.valid


def test_unknown_dependency_fails():
    template = make_template()
    steps = [
        OrderTemplateStep(
            template_id=template.id,
            name="payment",
            handler_class="tests.PaymentHandler",
            terminal_states=["paid"],
            advance_states=["paid"],
            depends_on=["missing"],
        )
    ]

    result = OrderTemplateValidator().validate(steps)

    assert not result.valid
    assert result.issues[0].code == "unknown_dependency"


def test_cycle_fails():
    template = make_template()
    first = OrderTemplateStep(
        template_id=template.id,
        name="first",
        handler_class="tests.FirstHandler",
        terminal_states=["done"],
        advance_states=["done"],
        depends_on=["second"],
    )
    second = OrderTemplateStep(
        template_id=template.id,
        name="second",
        handler_class="tests.SecondHandler",
        terminal_states=["done"],
        advance_states=["done"],
        depends_on=["first"],
    )

    result = OrderTemplateValidator().validate([first, second])

    assert not result.valid
    assert any(issue.code == "cycle_detected" for issue in result.issues)


def test_state_sets_must_be_terminal():
    template = make_template()
    step = OrderTemplateStep(
        template_id=template.id,
        name="payment",
        handler_class="tests.PaymentHandler",
        terminal_states=["paid"],
        advance_states=["processing"],
        rollback_states=["failed"],
    )

    result = OrderTemplateValidator().validate([step])

    assert not result.valid
    assert {issue.code for issue in result.issues} == {
        "advance_state_not_terminal",
        "rollback_state_not_terminal",
    }


def test_timeout_status_must_be_terminal():
    template = make_template()
    step = OrderTemplateStep(
        template_id=template.id,
        name="payment",
        handler_class="tests.PaymentHandler",
        terminal_states=["paid"],
        advance_states=["paid"],
        timeout_seconds=30,
        timeout_status="timeout",
    )

    result = OrderTemplateValidator().validate([step])

    assert not result.valid
    assert any(issue.code == "timeout_status_not_terminal" for issue in result.issues)


def test_flow_path_references_must_exist():
    _, steps = make_steps()
    flow_path = FlowPath(template_id=steps[0].template_id, name="fast", start_from="missing")

    result = OrderTemplateValidator().validate(steps, [flow_path])

    assert not result.valid
    assert any(issue.code == "unknown_start_from" for issue in result.issues)
