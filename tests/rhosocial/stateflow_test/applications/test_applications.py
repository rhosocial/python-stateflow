# tests/rhosocial/stateflow_test/applications/test_applications.py
"""Tests for the pre-built example components.

These tests double as documentation: each test shows how to use the
component end-to-end (configure backend → build template → create instance →
advance through the full workflow).
"""

from datetime import datetime, timedelta, timezone

import pytest

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    SyncOrderFactory,
    SyncOrderService,
    SyncTimeoutScheduler,
    create_tables,
    drop_tables,
    Order,
    OrderSubProcess,
)
from rhosocial.stateflow.applications import AgentPlan, ApprovalFlow, TaskOrchestration, TicketSystem


@pytest.fixture
def backend_group():
    """Shared backend for sync example tests."""
    config = SQLiteConnectionConfig(database=":memory:")
    with BackendGroup(name="examples", models=list(ApprovalFlow.models),
                      config=config, backend_class=SQLiteBackend) as g:
        b = g.get_backend()
        b.connect()
        b.introspect_and_adapt()
        create_tables(b)
        yield g
        drop_tables(b)


def _persist(instance):
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()


# ===========================================================================
# ApprovalFlow
# ===========================================================================


class TestApprovalFlow:
    def test_happy_path(self, backend_group):
        """submit → review → publish → order completed."""
        template, steps = ApprovalFlow.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"title": "RFC-001"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("submit").id,
                          new_status="submitted", event_key="ap-submit-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("review").id,
                          new_status="approved", event_key="ap-review-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("publish").id,
                          new_status="published", event_key="ap-publish-1")

        order = Order.query().where(Order.c.id == order_id).one()
        assert order.status == "completed"

    def test_reject_and_rollback(self, backend_group):
        """review rejected → rollback review."""
        template, steps = ApprovalFlow.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"title": "RFC-002"})
        _persist(instance)

        # Make review reversible
        review_sp = instance.get_subprocess("review")
        review_sp.is_reversible = True
        review_sp.save()

        svc = SyncOrderService()
        order_id = instance.order.id
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("submit").id,
                          new_status="submitted", event_key="ap2-submit-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=review_sp.id,
                          new_status="rejected", event_key="ap2-reject-1")

        result = svc.publish_rollback(order_id=order_id, subprocess_id=review_sp.id,
                                      event_key="ap2-rollback-1")
        assert result.event.event_type == "sp_rollback_started"

        reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == review_sp.id).one()
        assert reloaded.rollback_status == "running"

    def test_idempotent(self, backend_group):
        """Duplicate event_key returns duplicate=True."""
        template, steps = ApprovalFlow.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"title": "RFC-003"})
        _persist(instance)

        svc = SyncOrderService()
        submit_id = instance.get_subprocess("submit").id
        order_id = instance.order.id

        first = svc.publish_event(order_id=order_id, subprocess_id=submit_id,
                                  new_status="submitted", event_key="ap3-idem")
        second = svc.publish_event(order_id=order_id, subprocess_id=submit_id,
                                   new_status="submitted", event_key="ap3-idem")
        assert first.duplicate is False
        assert second.duplicate is True


# ===========================================================================
# TicketSystem
# ===========================================================================


class TestTicketSystem:
    def test_parallel_completion(self, backend_group):
        """create → assign → [dev_fix || qa_verify] → close → completed."""
        template, steps = TicketSystem.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"ticket_id": "T-100"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("create").id,
                          new_status="created", event_key="tk-create-1")

        result = svc.publish_event(order_id=order_id,
                                   subprocess_id=instance.get_subprocess("assign").id,
                                   new_status="assigned", event_key="tk-assign-1")
        # dev_fix and qa_verify should both be started
        started = sorted(sp.step_name for sp in result.started_subprocesses)
        assert started == ["dev_fix", "qa_verify"]

        # dev_fix → fixed (close not ready yet)
        r2 = svc.publish_event(order_id=order_id,
                               subprocess_id=instance.get_subprocess("dev_fix").id,
                               new_status="fixed", event_key="tk-devfix-1")
        assert r2.started_subprocesses == []

        # qa_verify → verified (close ready now)
        r3 = svc.publish_event(order_id=order_id,
                               subprocess_id=instance.get_subprocess("qa_verify").id,
                               new_status="verified", event_key="tk-qa-1")
        assert any(sp.step_name == "close" for sp in r3.started_subprocesses)

        # close → closed
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("close").id,
                          new_status="closed", event_key="tk-close-1")
        assert Order.query().where(Order.c.id == order_id).one().status == "completed"

    def test_skip_qa(self, backend_group):
        """Skip qa_verify → dev_fix advance triggers close."""
        template, steps = TicketSystem.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, skip_steps=["qa_verify"],
                                             context={"ticket_id": "T-101"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("create").id,
                          new_status="created", event_key="tk2-create-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("assign").id,
                          new_status="assigned", event_key="tk2-assign-1")
        result = svc.publish_event(order_id=order_id,
                                   subprocess_id=instance.get_subprocess("dev_fix").id,
                                   new_status="fixed", event_key="tk2-devfix-1")
        assert any(sp.step_name == "close" for sp in result.started_subprocesses)

    def test_dynamic_append(self, backend_group):
        """Append a notify step after close at runtime."""
        template, steps = TicketSystem.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"ticket_id": "T-102"})
        _persist(instance)

        close_sp = instance.get_subprocess("close")
        new_sp = SyncOrderFactory().append_subprocess(
            process=instance.process,
            existing_subprocesses=instance.subprocesses,
            existing_dependencies=instance.dependencies,
            name="notify",
            handler_class="examples.NotifyHandler",
            terminal_states=["sent"],
            advance_states=["sent"],
            depends_on=[close_sp],
        )
        new_sp.save()
        assert new_sp.source == "dynamic"
        assert new_sp.sequence == 5


# ===========================================================================
# TaskOrchestration
# ===========================================================================


class TestTaskOrchestration:
    def test_diamond_dag(self, backend_group):
        """prepare → [build || test] → deploy."""
        template, steps = TaskOrchestration.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"pipeline": "P-1"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("prepare").id,
                          new_status="ready", event_key="to-prepare-1")

        result = svc.publish_event(order_id=order_id,
                                   subprocess_id=instance.get_subprocess("build").id,
                                   new_status="built", event_key="to-build-1")
        # build alone doesn't start deploy (test still pending)
        assert result.started_subprocesses == []

        # test → tested (deploy should start)
        result2 = svc.publish_event(order_id=order_id,
                                    subprocess_id=instance.get_subprocess("test").id,
                                    new_status="tested", event_key="to-test-1")
        assert any(sp.step_name == "deploy" for sp in result2.started_subprocesses)

        # deploy → deployed → completed
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("deploy").id,
                          new_status="deployed", event_key="to-deploy-1")
        assert Order.query().where(Order.c.id == order_id).one().status == "completed"

    def test_timeout(self, backend_group):
        """Test stage times out → timeout_status."""
        template, steps = TaskOrchestration.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"pipeline": "P-2"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("prepare").id,
                          new_status="ready", event_key="to2-prepare-1")

        # test was auto-started by dispatcher; expire its timeout_at
        test_sp = instance.get_subprocess("test")
        test_db = OrderSubProcess.query().where(OrderSubProcess.c.id == test_sp.id).one()
        test_db.timeout_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        test_db.save()

        scheduler = SyncTimeoutScheduler(svc)
        processed = scheduler.tick()
        assert processed == 1

        reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == test_sp.id).one()
        assert reloaded.status == "timeout"


# ===========================================================================
# AgentPlan
# ===========================================================================


class TestAgentPlan:
    def test_happy_path(self, backend_group):
        """gather → analyze → execute → verify → completed."""
        template, steps = AgentPlan.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"task": "summarize doc"})
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("gather_context").id,
                          new_status="gathered", event_key="ag-gather-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("analyze").id,
                          new_status="analyzed", event_key="ag-analyze-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("execute_action").id,
                          new_status="executed", event_key="ag-execute-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("verify_result").id,
                          new_status="verified", event_key="ag-verify-1")
        assert Order.query().where(Order.c.id == order_id).one().status == "completed"

    def test_failure_and_compensation(self, backend_group):
        """execute_action fails → rollback in reverse order."""
        template, steps = AgentPlan.build_template()
        template.save()
        for s in steps:
            s.save()
        instance = SyncOrderFactory().create(template, steps, context={"task": "risky op"})
        # Make all steps reversible
        for sp in instance.subprocesses:
            sp.is_reversible = True
        _persist(instance)

        svc = SyncOrderService()
        order_id = instance.order.id

        # Advance gather + analyze
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("gather_context").id,
                          new_status="gathered", event_key="ag2-gather-1")
        svc.publish_event(order_id=order_id,
                          subprocess_id=instance.get_subprocess("analyze").id,
                          new_status="analyzed", event_key="ag2-analyze-1")

        # execute_action → execute_failed
        exec_sp = instance.get_subprocess("execute_action")
        svc.publish_event(order_id=order_id, subprocess_id=exec_sp.id,
                          new_status="execute_failed", event_key="ag2-execute-fail")

        # rollback execute_action
        r1 = svc.publish_rollback(order_id=order_id, subprocess_id=exec_sp.id,
                                  event_key="ag2-rb-execute-1")
        assert r1.event.event_type == "sp_rollback_started"

        # rollback analyze (it's in "analyzed", not a rollback_state!)
        analyze_sp = instance.get_subprocess("analyze")
        reloaded = OrderSubProcess.query().where(OrderSubProcess.c.id == analyze_sp.id).one()
        # "analyzed" is NOT in rollback_states ["analyze_failed"]
        assert not reloaded.can_rollback()

        # To rollback analyze we'd need it in analyze_failed first,
        # which demonstrates the state-machine guard.
