# src/rhosocial/stateflow/applications/task_orchestration.py
"""Pre-built task orchestration: DAG pipeline with timeout and rollback.

A three-stage pipeline where stage 2 has a timeout and all stages are
reversible. Demonstrates:

- Diamond DAG (prepare → [build, test] → deploy)
- Timeout (test stage: timeout_seconds + timeout_status)
- Rollback (deploy fails → rollback deploy + test + build)

Usage (sync):

```python
from rhosocial.stateflow import Schema, SyncOrderFactory, SyncOrderService
from rhosocial.stateflow.applications import TaskOrchestration

template, steps = TaskOrchestration.build_template()
# persist + use with TaskOrchestration.sync_registry()
```
"""

from typing import Optional, Tuple

from ..handlers import AsyncSubProcessHandler, SyncSubProcessHandler
from ..registry import HandlerRegistry
from rhosocial.stateflow.models.flow_path import AsyncFlowPath, FlowPath
from rhosocial.stateflow.models.order import AsyncOrder, Order
from rhosocial.stateflow.models.order_event import AsyncOrderEvent, OrderEvent
from rhosocial.stateflow.models.order_outbox import AsyncOrderOutbox, OrderOutbox
from rhosocial.stateflow.models.order_process import AsyncOrderProcess, OrderProcess
from rhosocial.stateflow.models.order_subprocess import AsyncOrderSubProcess, OrderSubProcess
from rhosocial.stateflow.models.order_template import AsyncOrderTemplate, OrderTemplate
from rhosocial.stateflow.models.order_template_step import AsyncOrderTemplateStep, OrderTemplateStep
from rhosocial.stateflow.models.subprocess_dependency import AsyncSubProcessDependency, SubProcessDependency
from ..types import HandlerResult

__all__ = ["TaskOrchestration"]


_TASK_STEP_DEFS: list = [
    {
        "name": "prepare",
        "handler_class": "stateflow.applications.task_orchestration.PrepareHandler",
        "terminal_states": ["ready", "prepare_failed"],
        "advance_states": ["ready"],
        "rollback_states": ["prepare_failed"],
        "step_order": 1,
    },
    {
        "name": "build",
        "handler_class": "stateflow.applications.task_orchestration.BuildHandler",
        "terminal_states": ["built", "build_failed"],
        "advance_states": ["built"],
        "rollback_states": ["build_failed"],
        "depends_on": ["prepare"],
        "step_order": 2,
    },
    {
        "name": "test",
        "handler_class": "stateflow.applications.task_orchestration.TestHandler",
        "terminal_states": ["tested", "test_failed", "timeout"],
        "advance_states": ["tested"],
        "rollback_states": ["test_failed", "timeout"],
        "timeout_seconds": 3600,
        "timeout_status": "timeout",
        "depends_on": ["prepare"],
        "step_order": 3,
    },
    {
        "name": "deploy",
        "handler_class": "stateflow.applications.task_orchestration.DeployHandler",
        "terminal_states": ["deployed", "deploy_failed"],
        "advance_states": ["deployed"],
        "rollback_states": ["deploy_failed"],
        "depends_on": ["build", "test"],
        "step_order": 4,
    },
]


class TaskOrchestration:
    """Pre-built task pipeline with diamond DAG, timeout, and rollback.

    Workflow::

        prepare ─┬→ build  ─┐
                 └→ test   ─┴→ deploy

    - ``test`` has a 1-hour timeout; on expiry it transitions to ``timeout``
      (a rollback state), which can trigger compensation.
    - All stages are reversible; a ``deploy_failed`` can roll back deploy,
      and the upstream stages can be rolled back independently.
    """

    name = "task_orchestration"
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
        for defn in _TASK_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps


    @classmethod
    def build_async_template(cls):
        """Create an in-memory async template + async steps (not persisted)."""
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _TASK_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def sync_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _TASK_STEP_DEFS:
            reg.register(defn["handler_class"], _SYNC_HANDLERS[defn["name"]])
        return reg

    @classmethod
    def async_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _TASK_STEP_DEFS:
            reg.register(defn["handler_class"], _ASYNC_HANDLERS[defn["name"]])
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class PrepareHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="ready", event_key=f"prepare-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class BuildHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="built", event_key=f"build-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class TestHandler(SyncSubProcessHandler):
    """Test stage — does not auto-complete.

    In a real system, ``start()`` returns ``None``; the test result
    (``tested`` / ``test_failed``) arrives from the CI system.
    The timeout sweeper will transition to ``timeout`` if no result arrives.
    """
    def start(self) -> None:
        return None
    def rollback(self) -> None:
        return None


class DeployHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="deployed", event_key=f"deploy-{self.subprocess.id}")
    def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="deploy_failed", event_key=f"deploy-rollback-{self.subprocess.id}")


_SYNC_HANDLERS = {
    "prepare": PrepareHandler,
    "build": BuildHandler,
    "test": TestHandler,
    "deploy": DeployHandler,
}


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncPrepareHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="ready", event_key=f"prepare-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncBuildHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="built", event_key=f"build-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncTestHandler(AsyncSubProcessHandler):
    async def start(self) -> None:
        return None
    async def rollback(self) -> None:
        return None


class AsyncDeployHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="deployed", event_key=f"deploy-{self.subprocess.id}")
    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="deploy_failed", event_key=f"deploy-rollback-{self.subprocess.id}")


_ASYNC_HANDLERS = {
    "prepare": AsyncPrepareHandler,
    "build": AsyncBuildHandler,
    "test": AsyncTestHandler,
    "deploy": AsyncDeployHandler,
}
