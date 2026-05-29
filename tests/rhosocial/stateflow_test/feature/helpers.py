# tests/rhosocial/stateflow_test/feature/helpers.py

from rhosocial.stateflow import OrderTemplate, OrderTemplateStep


def make_template():
    return OrderTemplate(name="purchase", version=1)


def make_steps():
    template = make_template()
    inventory = OrderTemplateStep(
        template_id=template.id,
        name="inventory",
        handler_class="tests.InventoryHandler",
        terminal_states=["locked", "failed"],
        advance_states=["locked"],
        rollback_states=["failed"],
        step_order=1,
    )
    payment = OrderTemplateStep(
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
    shipment = OrderTemplateStep(
        template_id=template.id,
        name="shipment",
        handler_class="tests.ShipmentHandler",
        terminal_states=["shipped", "failed"],
        advance_states=["shipped"],
        rollback_states=["failed"],
        depends_on=["payment"],
        step_order=3,
    )
    return template, [inventory, payment, shipment]
