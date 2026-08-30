# tests/rhosocial/stateflow_test/feature/test_dispatcher.py

import pytest

from rhosocial.stateflow import InvalidStateTransitionError, SyncOrderDispatcher, SyncOrderFactory

from .helpers import make_steps


def make_instance():
    template, steps = make_steps()
    return SyncOrderFactory().create(template, steps)


def test_event_key_is_idempotent():
    instance = make_instance()
    dispatcher = SyncOrderDispatcher()
    inventory = instance.get_subprocess("inventory")

    first = dispatcher.on_event(
        instance.order,
        inventory,
        new_status="locked",
        subprocesses=instance.subprocesses,
        dependencies=instance.dependencies,
        event_key="inventory-1",
    )
    second = dispatcher.on_event(
        instance.order,
        inventory,
        new_status="locked",
        subprocesses=instance.subprocesses,
        dependencies=instance.dependencies,
        events=[first.event],
        event_key="inventory-1",
    )

    assert second.duplicate
    assert second.event is first.event


def test_advance_state_starts_downstream_subprocess():
    instance = make_instance()
    dispatcher = SyncOrderDispatcher()
    inventory = instance.get_subprocess("inventory")
    payment = instance.get_subprocess("payment")

    result = dispatcher.on_event(
        instance.order,
        inventory,
        new_status="locked",
        subprocesses=instance.subprocesses,
        dependencies=instance.dependencies,
    )

    assert payment.status == "stateflow:subprocess:running"
    assert result.started_subprocesses == [payment]
    assert len(result.outbox_items) == 1
    assert result.outbox_items[0].topic == "stateflow:topic:handler_start"


def test_terminal_state_cannot_be_overwritten():
    instance = make_instance()
    dispatcher = SyncOrderDispatcher()
    inventory = instance.get_subprocess("inventory")

    dispatcher.on_event(
        instance.order,
        inventory,
        new_status="locked",
        subprocesses=instance.subprocesses,
        dependencies=instance.dependencies,
    )
    result = dispatcher.on_event(
        instance.order,
        inventory,
        new_status="failed",
        subprocesses=instance.subprocesses,
        dependencies=instance.dependencies,
    )

    assert result.event.conflict
    assert inventory.status == "locked"


def test_skipped_subprocess_rejects_events():
    template, steps = make_steps()
    instance = SyncOrderFactory().create(template, steps, skip_steps=["payment"])

    with pytest.raises(InvalidStateTransitionError):
        SyncOrderDispatcher().on_event(
            instance.order,
            instance.get_subprocess("payment"),
            new_status="paid",
            subprocesses=instance.subprocesses,
            dependencies=instance.dependencies,
        )
