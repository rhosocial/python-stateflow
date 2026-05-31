import uuid

from rhosocial.stateflow import Order, OrderProcess, OrderTemplate, OrderTemplateStep


def test_from_template_binds_order_and_snapshot():
    order = Order(template_id=uuid.uuid4())
    template = OrderTemplate(name="demo")
    step = OrderTemplateStep(
        template_id=template.id,
        name="inventory",
        handler_class="tests.Handler",
        terminal_states=["done"],
        advance_states=["done"],
    )

    process = OrderProcess.from_template(order, template, [step])

    assert process.order_id == order.id
    assert process.template_snapshot["template"]["name"] == "demo"
    assert process.template_snapshot["steps"][0]["name"] == "inventory"
