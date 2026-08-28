# src/rhosocial/stateflow/applications/agent_plan.py
"""Pre-built agent execution plan: step orchestration with failure compensation.

A linear plan where each agent step can fail and trigger compensation
(rollback). If any step fails, all previously completed reversible steps
are rolled back in reverse order.

Usage (sync):

```python
from rhosocial.stateflow import Schema, SyncOrderFactory, SyncOrderService
from rhosocial.stateflow.applications import AgentPlan

template, steps = AgentPlan.build_template()
# persist + use with AgentPlan.sync_registry()
```
"""

from typing import Optional, Tuple

from ..handlers import AsyncSubProcessHandler, SyncSubProcessHandler
from ..registry import HandlerRegistry
from ..models import (
    AsyncFlowPath,
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderOutbox,
    AsyncOrderProcess,
    AsyncOrderSubProcess,
    AsyncOrderTemplate,
    AsyncOrderTemplateStep,
    AsyncSubProcessDependency,
    FlowPath,
    Order,
    OrderEvent,
    OrderOutbox,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
)
from ..types import HandlerResult

__all__ = ["AgentPlan"]


_AGENT_STEP_DEFS: list = [
    {
        "name": "gather_context",
        "handler_class": "stateflow.applications.agent_plan.GatherContextHandler",
        "terminal_states": ["gathered", "gather_failed"],
        "advance_states": ["gathered"],
        "rollback_states": ["gather_failed"],
        "step_order": 1,
    },
    {
        "name": "analyze",
        "handler_class": "stateflow.applications.agent_plan.AnalyzeHandler",
        "terminal_states": ["analyzed", "analyze_failed"],
        "advance_states": ["analyzed"],
        "rollback_states": ["analyze_failed"],
        "depends_on": ["gather_context"],
        "step_order": 2,
    },
    {
        "name": "execute_action",
        "handler_class": "stateflow.applications.agent_plan.ExecuteActionHandler",
        "terminal_states": ["executed", "execute_failed"],
        "advance_states": ["executed"],
        "rollback_states": ["execute_failed"],
        "depends_on": ["analyze"],
        "step_order": 3,
    },
    {
        "name": "verify_result",
        "handler_class": "stateflow.applications.agent_plan.VerifyResultHandler",
        "terminal_states": ["verified", "verify_failed"],
        "advance_states": ["verified"],
        "rollback_states": ["verify_failed"],
        "depends_on": ["execute_action"],
        "step_order": 4,
    },
]


class AgentPlan:
    """Pre-built agent execution plan with failure compensation.

    Workflow::

        gather_context → analyze → execute_action → verify_result

    Each step's failure state (``*_failed``) is a ``rollback_state``,
    so a failure at any point can trigger ``publish_rollback`` to
    compensate upstream steps in reverse order.

    Example failure scenario:

    1. ``gather_context`` → ``gathered``
    2. ``analyze`` → ``analyzed``
    3. ``execute_action`` → ``execute_failed``
    4. ``publish_rollback(execute_action)`` → handler compensates
    5. ``publish_rollback(analyze)`` → handler compensates
    6. ``publish_rollback(gather_context)`` → handler compensates
    """

    name = "agent_plan"
    version = 1

    models = (
        OrderTemplate, OrderTemplateStep, FlowPath,
        Order, OrderProcess, OrderSubProcess,
        SubProcessDependency, OrderEvent, OrderOutbox,
    )

    async_models = (
        AsyncOrderTemplate, AsyncOrderTemplateStep, AsyncFlowPath,
        AsyncOrder, AsyncOrderProcess, AsyncOrderSubProcess,
        AsyncSubProcessDependency, AsyncOrderEvent, AsyncOrderOutbox,
    )

    @classmethod
    def build_template(cls) -> Tuple[OrderTemplate, list]:
        template = OrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _AGENT_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps


    @classmethod
    def build_async_template(cls):
        """Create an in-memory async template + async steps (not persisted)."""
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _AGENT_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def sync_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _AGENT_STEP_DEFS:
            reg.register(defn["handler_class"], _SYNC_HANDLERS[defn["name"]])
        return reg

    @classmethod
    def async_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _AGENT_STEP_DEFS:
            reg.register(defn["handler_class"], _ASYNC_HANDLERS[defn["name"]])
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class GatherContextHandler(SyncSubProcessHandler):
    """Gathers context (e.g. search, file read) for the agent."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="gathered", event_key=f"gather-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        # Compensation: discard gathered context
        return HandlerResult(status="gather_failed", event_key=f"gather-rollback-{self.subprocess.id}")


class AnalyzeHandler(SyncSubProcessHandler):
    """Analyzes the gathered context."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="analyzed", event_key=f"analyze-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="analyze_failed", event_key=f"analyze-rollback-{self.subprocess.id}")


class ExecuteActionHandler(SyncSubProcessHandler):
    """Executes the planned action.

    In a real agent, ``start()`` would return ``None`` and the result
    would arrive from an external tool call. This handler auto-completes
    for demonstration; override to inject failures.
    """

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="executed", event_key=f"execute-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="execute_failed", event_key=f"execute-rollback-{self.subprocess.id}")


class VerifyResultHandler(SyncSubProcessHandler):
    """Verifies the action result."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verified", event_key=f"verify-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verify_failed", event_key=f"verify-rollback-{self.subprocess.id}")


_SYNC_HANDLERS = {
    "gather_context": GatherContextHandler,
    "analyze": AnalyzeHandler,
    "execute_action": ExecuteActionHandler,
    "verify_result": VerifyResultHandler,
}


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncGatherContextHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="gathered", event_key=f"gather-{self.subprocess.id}")
    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="gather_failed", event_key=f"gather-rollback-{self.subprocess.id}")


class AsyncAnalyzeHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="analyzed", event_key=f"analyze-{self.subprocess.id}")
    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="analyze_failed", event_key=f"analyze-rollback-{self.subprocess.id}")


class AsyncExecuteActionHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="executed", event_key=f"execute-{self.subprocess.id}")
    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="execute_failed", event_key=f"execute-rollback-{self.subprocess.id}")


class AsyncVerifyResultHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verified", event_key=f"verify-{self.subprocess.id}")
    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verify_failed", event_key=f"verify-rollback-{self.subprocess.id}")


_ASYNC_HANDLERS = {
    "gather_context": AsyncGatherContextHandler,
    "analyze": AsyncAnalyzeHandler,
    "execute_action": AsyncExecuteActionHandler,
    "verify_result": AsyncVerifyResultHandler,
}
