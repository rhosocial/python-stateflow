import uuid

import pytest

from rhosocial.stateflow import OrderTemplate, OrderTemplateStep, TemplateValidationError


def make_step(name, order):
    return OrderTemplateStep(
        template_id=uuid.uuid4(),
        name=name,
        handler_class="tests.Handler",
        terminal_states=["done", "failed"],
        advance_states=["done"],
        rollback_states=["failed"],
        step_order=order,
    )


def test_ordered_steps_sort_by_step_order():
    template = OrderTemplate(name="demo")
    steps = [make_step("second", 2), make_step("first", 1)]

    ordered = template.ordered_steps(steps)

    assert [step.name for step in ordered] == ["first", "second"]


def test_steps_before_returns_names_before_entry_point():
    template = OrderTemplate(name="demo")
    steps = [make_step("first", 1), make_step("second", 2), make_step("third", 3)]

    assert template.steps_before(steps, "third") == {"first", "second"}


def test_steps_before_rejects_unknown_entry_point():
    template = OrderTemplate(name="demo")

    with pytest.raises(TemplateValidationError):
        template.steps_before([make_step("first", 1)], "missing")


def test_snapshot_captures_template_and_step_fields():
    template = OrderTemplate(name="demo", version=2)
    step = make_step("first", 1)

    snapshot = template.snapshot([step])

    assert snapshot["template"]["name"] == "demo"
    assert snapshot["template"]["version"] == 2
    assert snapshot["steps"][0]["name"] == "first"
    assert snapshot["steps"][0]["advance_states"] == ["done"]
