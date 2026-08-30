# tests/rhosocial/stateflow_test/applications/test_seat_booking.py
"""Tests for the SeatBookingFlow application component (sync + async)."""

import pytest

from rhosocial.stateflow import (
    SyncOrderFactory, SyncOrderService, AsyncOrderFactory, AsyncOrderService,
    Order, AsyncOrder,
)
from rhosocial.stateflow.applications import SeatBookingFlow
from rhosocial.stateflow.applications.external_services import MockPaymentService, AsyncMockPaymentService





def _persist(instance):
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()


class TestSeatBookingSync:
    def test_happy_path(self, backend_group):
        """select → validate → pay → issue_ticket → completed."""
        payment = MockPaymentService()
        template, steps = SeatBookingFlow.build_template()
        template.save()
        for s in steps:
            s.save()

        instance = SyncOrderFactory().create(template, steps, context={
            "seat_id": "A-12", "price": 8800, "event": "concert",
        })
        for sp in instance.subprocesses:
            sp.extra = {"seat_id": "A-12", "price": 8800}
            sp.save()
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        # Simulate payment
        pay_sp = instance.get_subprocess("payment")
        tx_id = payment.charge(order_id, 8800)
        pay_sp.extra["tx_id"] = tx_id
        pay_sp.save()

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("select_seat").id,
                          new_status="seat_selected", event_key="sb-seat")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("validate").id,
                          new_status="validated", event_key="sb-validate")
        svc.publish_event(order_id=order_id,
                          subprocess_id=pay_sp.id,
                          new_status="paid", event_key="sb-pay")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("issue_ticket").id,
                          new_status="ticketed", event_key="sb-ticket")

        order = Order.query().where(Order.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

    def test_payment_failure_and_rollback(self, backend_group):
        """select → validate → payment_failed → rollback (refund)."""
        template, steps = SeatBookingFlow.build_template()
        template.save()
        for s in steps:
            s.save()

        instance = SyncOrderFactory().create(template, steps, context={
            "seat_id": "B-5", "price": 5000,
        })
        for sp in instance.subprocesses:
            sp.extra = {"seat_id": "B-5", "price": 5000}
            sp.save()
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("select_seat").id,
                          new_status="seat_selected", event_key="sb2-seat")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("validate").id,
                          new_status="validated", event_key="sb2-validate")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("payment").id,
                          new_status="payment_failed", event_key="sb2-pay-fail")

        # payment reached a rollback_state — make it reversible and rollback
        pay_db = type(instance.get_subprocess("payment")).query().where(
            type(instance.get_subprocess("payment")).c.id == instance.get_subprocess("payment").id
        ).one()
        pay_db.is_reversible = True
        pay_db.save()

        result = svc.publish_rollback(order_id=order_id,
                                       subprocess_id=instance.get_subprocess("payment").id,
                                       event_key="sb2-rollback")
        assert result.event.event_type == "stateflow:event:sp_rollback_started"





async def _async_persist(instance):
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()
    for d in instance.dependencies:
        await d.save()
    for e in instance.events:
        await e.save()


class TestSeatBookingAsync:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_backend_group):
        template, steps = SeatBookingFlow.build_async_template()
        await template.save()
        for s in steps:
            await s.save()

        instance = await AsyncOrderFactory().create(template, steps, context={
            "seat_id": "C-1", "price": 3000,
        })
        for sp in instance.subprocesses:
            sp.extra = {"seat_id": "C-1", "price": 3000}
            await sp.save()
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        # Simulate payment
        pay_sp = instance.get_subprocess("payment")
        ps = AsyncMockPaymentService()
        tx_id = await ps.charge(order_id, 3000)
        pay_sp.extra["tx_id"] = tx_id
        await pay_sp.save()

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("select_seat").id,
                                new_status="seat_selected", event_key="sba-seat")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("validate").id,
                                new_status="validated", event_key="sba-validate")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=pay_sp.id,
                                new_status="paid", event_key="sba-pay")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("issue_ticket").id,
                                new_status="ticketed", event_key="sba-ticket")

        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"
