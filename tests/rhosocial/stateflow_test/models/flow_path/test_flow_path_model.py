import uuid

from rhosocial.stateflow import FlowPath


def test_has_unknown_start_from_detects_missing_entry_point():
    flow_path = FlowPath(template_id=uuid.uuid4(), name="resume", start_from="payment")

    assert flow_path.has_unknown_start_from({"inventory"})
    assert not flow_path.has_unknown_start_from({"inventory", "payment"})


def test_unknown_skip_steps_returns_only_missing_steps():
    flow_path = FlowPath(
        template_id=uuid.uuid4(),
        name="fast-path",
        skip_steps=["inventory", "shipment"],
    )

    assert flow_path.unknown_skip_steps({"inventory", "payment"}) == ["shipment"]
