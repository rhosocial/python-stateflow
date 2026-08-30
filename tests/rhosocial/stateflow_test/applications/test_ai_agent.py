# tests/rhosocial/stateflow_test/applications/test_ai_agent.py
"""Tests for the runtime-defined execution graph demo (AiAgentAssistant).

These tests double as documentation: they show how ``AiAgentAssistant.run`` /
``run_async`` dynamically append a tool chain after the order is created,
drive it to completion, and compensate upstream steps on failure.
"""

from rhosocial.stateflow import SyncOrderService
from rhosocial.stateflow.applications import AiAgentAssistant
from rhosocial.stateflow.models import (
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderSubProcess,
    Order,
    OrderEvent,
    OrderSubProcess,
)


class TestAiAgentAssistant:
    def test_happy_path_builds_dynamic_graph(self, backend_group):
        """plan → [append search/write/review] → completed with sp_appended events."""
        service = SyncOrderService()
        order_id = AiAgentAssistant.run(
            service, AiAgentAssistant.sync_registry(), {"task": "summarize the RFC"}
        )

        order = Order.query().where(Order.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

        subprocesses = OrderSubProcess.query().all()
        by_name = {sp.step_name: sp for sp in subprocesses}
        assert set(by_name) == {"plan", "search", "write", "review"}
        assert by_name["plan"].source == "stateflow:source:template"
        assert by_name["search"].source == "stateflow:source:dynamic"
        assert by_name["write"].status == "written"
        assert by_name["review"].status == "reviewed"

        event_types = {e.event_type for e in OrderEvent.query().all()}
        assert "stateflow:event:sp_appended" in event_types

    def test_quick_context_skips_search(self, backend_group):
        """Dynamic plan: quick=True drops the search step."""
        service = SyncOrderService()
        order_id = AiAgentAssistant.run(
            service,
            AiAgentAssistant.sync_registry(),
            {"task": "quick one", "quick": True},
        )

        subprocesses = OrderSubProcess.query().all()
        assert {sp.step_name for sp in subprocesses} == {"plan", "write", "review"}
        assert Order.query().where(Order.c.id == order_id).one().status == "stateflow:order:completed"

    def test_failure_compensates_upstream_in_reverse(self, backend_group):
        """write fails → write and search are rolled back; review is not started."""
        service = SyncOrderService()
        order_id = AiAgentAssistant.run(
            service,
            AiAgentAssistant.sync_registry(),
            {"task": "risky op"},
            fail_step="write",
        )

        order = Order.query().where(Order.c.id == order_id).one()
        assert order.status == "stateflow:order:pending"

        subprocesses = OrderSubProcess.query().all()
        by_name = {sp.step_name: sp for sp in subprocesses}
        assert by_name["write"].status == "write_failed"
        assert by_name["write"].rollback_status == "stateflow:rollback:running"
        assert by_name["search"].rollback_status == "stateflow:rollback:running"
        # downstream review never started (write is not an advance state)
        assert by_name["review"].status == "stateflow:subprocess:pending"

        event_types = {e.event_type for e in OrderEvent.query().all()}
        assert "stateflow:event:sp_rollback_started" in event_types


class TestAsyncAiAgentAssistant:
    async def test_async_happy_path(self, async_backend_group):
        """Async counterpart: dynamic graph completes end-to-end."""
        from rhosocial.stateflow import AsyncOrderService

        svc = AsyncOrderService()
        order_id = await AiAgentAssistant.run_async(
            svc, AiAgentAssistant.async_registry(), {"task": "async task"}
        )

        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

        subprocesses = await AsyncOrderSubProcess.query().all()
        by_name = {sp.step_name: sp for sp in subprocesses}
        assert set(by_name) == {"plan", "search", "write", "review"}
        assert by_name["review"].status == "reviewed"

        event_types = {e.event_type for e in await AsyncOrderEvent.query().all()}
        assert "stateflow:event:sp_appended" in event_types

    async def test_async_failure_compensates_upstream(self, async_backend_group):
        """Async failure path: upstream steps enter rollback."""
        from rhosocial.stateflow import AsyncOrderService

        svc = AsyncOrderService()
        order_id = await AiAgentAssistant.run_async(
            svc,
            AiAgentAssistant.async_registry(),
            {"task": "async risky"},
            fail_step="review",
        )

        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:pending"

        subprocesses = await AsyncOrderSubProcess.query().all()
        by_name = {sp.step_name: sp for sp in subprocesses}
        assert by_name["review"].status == "review_failed"
        assert by_name["review"].rollback_status == "stateflow:rollback:running"
        assert by_name["write"].rollback_status == "stateflow:rollback:running"
        assert by_name["search"].rollback_status == "stateflow:rollback:running"
