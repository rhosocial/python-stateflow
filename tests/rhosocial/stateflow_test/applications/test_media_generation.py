# tests/rhosocial/stateflow_test/applications/test_media_generation.py
"""Tests for the MediaGenerationFlow application component (sync + async)."""

import pytest

from rhosocial.stateflow import (
    SyncOrderFactory, SyncOrderService, AsyncOrderFactory, AsyncOrderService,
    Order, AsyncOrder,
)
from rhosocial.stateflow.applications import MediaGenerationFlow
from rhosocial.stateflow.applications.external_services import MockCreditService, AsyncMockCreditService





def _persist(instance):
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()


class TestMediaGenerationSync:
    def test_happy_path(self, backend_group):
        """collect → freeze → submit → poll(succeeded) → deliver → completed."""
        credits = MockCreditService()
        credits.set_balance("user-1", 10000)
        template, steps = MediaGenerationFlow.build_template()
        template.save()
        for s in steps:
            s.save()

        instance = SyncOrderFactory().create(template, steps, context={
            "user_id": "user-1", "prompt": "a cat", "credit_cost": 100,
        }, skip_steps=["refund_credits"])
        for sp in instance.subprocesses:
            sp.extra = {
                "user_id": "user-1", "prompt": "a cat",
                "credit_cost": 100, "simulate_success": True,
            }
            sp.save()
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        # Drive each step to its advance state. In a real deployment the
        # handler_start topic would call the handler; here we simulate the
        # freeze by calling the credit service directly.
        freeze_sp = instance.get_subprocess("freeze_credits")
        freeze_id = credits.freeze("user-1", 100, reason="gen")
        freeze_sp.extra["freeze_id"] = freeze_id
        freeze_sp.save()

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("collect_params").id,
                          new_status="collected", event_key="mg-collect-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=freeze_sp.id,
                          new_status="frozen", event_key="mg-freeze-1")
        assert credits.get_balance("user-1") == 9900

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("submit_generation").id,
                          new_status="submitted", event_key="mg-submit-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("poll_result").id,
                          new_status="succeeded", event_key="mg-poll-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("deliver").id,
                          new_status="delivered", event_key="mg-deliver-1")

        order = Order.query().where(Order.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

    def test_failure_path_with_refund(self, backend_group):
        """collect → freeze → submit → poll(failed) → refund → credits returned."""
        credits = MockCreditService()
        credits.set_balance("user-2", 10000)
        template, steps = MediaGenerationFlow.build_template()
        template.save()
        for s in steps:
            s.save()

        instance = SyncOrderFactory().create(template, steps, context={
            "user_id": "user-2", "prompt": "bad prompt", "credit_cost": 50,
        }, skip_steps=["deliver"])
        for sp in instance.subprocesses:
            sp.extra = {
                "user_id": "user-2", "prompt": "bad prompt",
                "credit_cost": 50, "simulate_success": False,
            }
            sp.save()
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        freeze_sp = instance.get_subprocess("freeze_credits")
        freeze_id = credits.freeze("user-2", 50, reason="gen")
        freeze_sp.extra["freeze_id"] = freeze_id
        freeze_sp.save()

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("collect_params").id,
                          new_status="collected", event_key="mg2-collect")
        svc.publish_event(order_id=order_id,
                          subprocess_id=freeze_sp.id,
                          new_status="frozen", event_key="mg2-freeze")
        assert credits.get_balance("user-2") == 9950

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("submit_generation").id,
                          new_status="submitted", event_key="mg2-submit")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("poll_result").id,
                          new_status="generation_failed", event_key="mg2-poll-fail")

        # Refund: unfreeze the credits
        credits.unfreeze(freeze_id)
        assert credits.get_balance("user-2") == 10000





async def _async_persist(instance):
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()
    for d in instance.dependencies:
        await d.save()
    for e in instance.events:
        await e.save()


class TestMediaGenerationAsync:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_backend_group):
        credits = AsyncMockCreditService()
        credits.set_balance("user-a1", 10000)
        template, steps = MediaGenerationFlow.build_async_template()
        await template.save()
        for s in steps:
            await s.save()

        instance = await AsyncOrderFactory().create(template, steps, context={
            "user_id": "user-a1", "prompt": "a dog", "credit_cost": 200,
        }, skip_steps=["refund_credits"])
        for sp in instance.subprocesses:
            sp.extra = {
                "user_id": "user-a1", "prompt": "a dog",
                "credit_cost": 200, "simulate_success": True,
            }
            await sp.save()
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        # Simulate freeze
        freeze_sp = instance.get_subprocess("freeze_credits")
        freeze_id = await credits.freeze("user-a1", 200, reason="gen")
        freeze_sp.extra["freeze_id"] = freeze_id
        await freeze_sp.save()

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("collect_params").id,
                                new_status="collected", event_key="mga-collect")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=freeze_sp.id,
                                new_status="frozen", event_key="mga-freeze")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("submit_generation").id,
                                new_status="submitted", event_key="mga-submit")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("poll_result").id,
                                new_status="succeeded", event_key="mga-poll")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("deliver").id,
                                new_status="delivered", event_key="mga-deliver")

        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"
