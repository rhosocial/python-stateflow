# tests/rhosocial/stateflow_test/applications/test_applications_async.py
"""Async path tests for pre-built application components.

Mirrors ``test_applications.py`` but uses ``AsyncSQLiteBackend`` +
``AsyncBackendGroup`` + async models with native ``await`` throughout.
No ``asyncio.to_thread`` — genuine async I/O.
"""

from datetime import datetime, timedelta, timezone

import pytest

from rhosocial.stateflow import (
    AsyncOrderFactory,
    AsyncOrderService,
    AsyncTimeoutScheduler,
    AsyncOrder,
    AsyncOrderSubProcess,
)
from rhosocial.stateflow.applications import AgentPlan, ApprovalFlow, TaskOrchestration, TicketSystem





async def _async_persist(instance):
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()
    for d in instance.dependencies:
        await d.save()
    for e in instance.events:
        await e.save()


# ---------------------------------------------------------------------------
# ApprovalFlow (async)
# ---------------------------------------------------------------------------


class TestApprovalFlowAsync:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_backend_group):
        template, steps = ApprovalFlow.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"title": "RFC-A1"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("submit").id,
                                new_status="submitted", event_key="ap-async-submit")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("review").id,
                                new_status="approved", event_key="ap-async-review")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("publish").id,
                                new_status="published", event_key="ap-async-publish")

        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

    @pytest.mark.asyncio
    async def test_idempotent(self, async_backend_group):
        template, steps = ApprovalFlow.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"title": "RFC-A2"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        submit_id = instance.get_subprocess("submit").id
        order_id = instance.order.id

        first = await svc.publish_event(order_id=order_id, subprocess_id=submit_id,
                                       new_status="submitted", event_key="ap-async-idem")
        second = await svc.publish_event(order_id=order_id, subprocess_id=submit_id,
                                         new_status="submitted", event_key="ap-async-idem")
        assert first.duplicate is False
        assert second.duplicate is True


# ---------------------------------------------------------------------------
# TicketSystem (async)
# ---------------------------------------------------------------------------


class TestTicketSystemAsync:
    @pytest.mark.asyncio
    async def test_parallel_completion(self, async_backend_group):
        template, steps = TicketSystem.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"ticket_id": "T-A1"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("create").id,
                                new_status="created", event_key="tk-async-create")
        result = await svc.publish_event(order_id=order_id,
                                         subprocess_id=instance.get_subprocess("assign").id,
                                         new_status="assigned", event_key="tk-async-assign")
        started = sorted(sp.step_name for sp in result.started_subprocesses)
        assert started == ["dev_fix", "qa_verify"]

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("dev_fix").id,
                                new_status="fixed", event_key="tk-async-devfix")
        result2 = await svc.publish_event(order_id=order_id,
                                          subprocess_id=instance.get_subprocess("qa_verify").id,
                                          new_status="verified", event_key="tk-async-qa")
        assert any(sp.step_name == "close" for sp in result2.started_subprocesses)

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("close").id,
                                new_status="closed", event_key="tk-async-close")
        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"


# ---------------------------------------------------------------------------
# TaskOrchestration (async)
# ---------------------------------------------------------------------------


class TestTaskOrchestrationAsync:
    @pytest.mark.asyncio
    async def test_diamond_dag(self, async_backend_group):
        template, steps = TaskOrchestration.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"pipeline": "P-A1"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("prepare").id,
                                new_status="ready", event_key="to-async-prepare")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("build").id,
                                new_status="built", event_key="to-async-build")
        result = await svc.publish_event(order_id=order_id,
                                         subprocess_id=instance.get_subprocess("test").id,
                                         new_status="tested", event_key="to-async-test")
        assert any(sp.step_name == "deploy" for sp in result.started_subprocesses)

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("deploy").id,
                                new_status="deployed", event_key="to-async-deploy")
        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

    @pytest.mark.asyncio
    async def test_timeout(self, async_backend_group):
        template, steps = TaskOrchestration.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"pipeline": "P-A2"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("prepare").id,
                                new_status="ready", event_key="to2-async-prepare")

        test_sp = instance.get_subprocess("test")
        test_db = await AsyncOrderSubProcess.query().where(
            AsyncOrderSubProcess.c.id == test_sp.id
        ).one()
        test_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        await test_db.save()

        scheduler = AsyncTimeoutScheduler(svc)
        processed = await scheduler.tick()
        assert processed == 1

        reloaded = await AsyncOrderSubProcess.query().where(
            AsyncOrderSubProcess.c.id == test_sp.id
        ).one()
        assert reloaded.status == "timeout"


# ---------------------------------------------------------------------------
# AgentPlan (async)
# ---------------------------------------------------------------------------


class TestAgentPlanAsync:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_backend_group):
        template, steps = AgentPlan.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"task": "async summarize"})
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("gather_context").id,
                                new_status="gathered", event_key="ag-async-gather")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("analyze").id,
                                new_status="analyzed", event_key="ag-async-analyze")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("execute_action").id,
                                new_status="executed", event_key="ag-async-execute")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("verify_result").id,
                                new_status="verified", event_key="ag-async-verify")
        order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
        assert order.status == "stateflow:order:completed"

    @pytest.mark.asyncio
    async def test_failure_and_rollback(self, async_backend_group):
        template, steps = AgentPlan.build_async_template()
        await template.save()
        for s in steps:
            await s.save()
        instance = await AsyncOrderFactory().create(template, steps, context={"task": "async risky"})
        for sp in instance.subprocesses:
            sp.is_reversible = True
        await _async_persist(instance)

        svc = AsyncOrderService()
        order_id = instance.order.id

        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("gather_context").id,
                                new_status="gathered", event_key="ag2-async-gather")
        await svc.publish_event(order_id=order_id,
                                subprocess_id=instance.get_subprocess("analyze").id,
                                new_status="analyzed", event_key="ag2-async-analyze")

        exec_sp = instance.get_subprocess("execute_action")
        await svc.publish_event(order_id=order_id, subprocess_id=exec_sp.id,
                                new_status="execute_failed", event_key="ag2-async-fail")

        result = await svc.publish_rollback(order_id=order_id, subprocess_id=exec_sp.id,
                                            event_key="ag2-async-rb")
        assert result.event.event_type == "stateflow:event:sp_rollback_started"

        reloaded = await AsyncOrderSubProcess.query().where(
            AsyncOrderSubProcess.c.id == exec_sp.id
        ).one()
        assert reloaded.rollback_status == "stateflow:rollback:running"
