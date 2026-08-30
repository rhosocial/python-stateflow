# tests/rhosocial/stateflow_test/feature/test_factory.py

from rhosocial.stateflow import SyncOrderFactory

from .helpers import make_steps


def test_create_order_instance():
    template, steps = make_steps()

    instance = SyncOrderFactory().create(template, steps, context={"user_id": "u123"})

    assert instance.order.template_id == template.id
    assert instance.order.context == {"user_id": "u123"}
    assert instance.process.order_id == instance.order.id
    assert [subprocess.step_name for subprocess in instance.subprocesses] == [
        "inventory",
        "payment",
        "shipment",
    ]
    assert len(instance.events) == 4


def test_skip_steps_propagates_dependencies():
    template, steps = make_steps()

    instance = SyncOrderFactory().create(template, steps, skip_steps=["payment"])
    shipment = instance.get_subprocess("shipment")
    inventory = instance.get_subprocess("inventory")

    assert instance.get_subprocess("payment").skipped
    assert any(
        dependency.subprocess_id == shipment.id and dependency.depends_on_id == inventory.id
        for dependency in instance.dependencies
    )


def test_start_from_skips_previous_steps():
    template, steps = make_steps()

    instance = SyncOrderFactory().create(template, steps, start_from="payment")

    assert instance.get_subprocess("inventory").skipped
    assert not instance.get_subprocess("payment").skipped
    assert not instance.get_subprocess("shipment").skipped


def test_append_subprocess_assigns_dynamic_source_and_sequence():
    template, steps = make_steps()
    instance = SyncOrderFactory().create(template, steps)

    new_subprocess = SyncOrderFactory().append_subprocess(
        instance.process,
        instance.subprocesses,
        instance.dependencies,
        name="notify",
        handler_class="tests.NotifyHandler",
        terminal_states=["sent"],
        advance_states=["sent"],
        depends_on=[instance.get_subprocess("shipment")],
    )

    assert new_subprocess.step_name == "notify"
    assert new_subprocess.source == "stateflow:source:dynamic"
    assert new_subprocess.sequence == 3
