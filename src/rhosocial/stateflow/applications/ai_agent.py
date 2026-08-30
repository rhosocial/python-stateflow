# src/rhosocial/stateflow/applications/ai_agent.py
"""Pre-built AI agent assistant with a runtime-defined execution graph.

A demo showcasing stateflow's ability to **define the execution graph at
runtime**: a seed ``plan`` step is expanded into a tool-call chain
(``search`` → ``write`` → ``review``) via ``service.append_subprocess``
after the order is created. Each tool step is a ``SyncSubProcessHandler`` /
``AsyncSubProcessHandler`` whose ``start()`` reports a terminal state; a
failure (``*_failed``) is a rollback state so upstream reversible steps can
be compensated in reverse order.

Usage (sync):

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.stateflow import Schema, SyncOrderService
from rhosocial.stateflow.applications import AiAgentAssistant

config = SQLiteConnectionConfig(database=":memory:")
with BackendGroup(name="stateflow", models=AiAgentAssistant.models,
                  config=config, backend_class=SQLiteBackend) as group:
    backend = group.get_backend()
    backend.connect(); backend.introspect_and_adapt()
    Schema.create_tables(backend)

    service = SyncOrderService()
    order_id = AiAgentAssistant.run(
        service, AiAgentAssistant.sync_registry(), {"task": "summarize the RFC"}
    )
```
"""

from typing import Any, Dict, List, Optional, Tuple

from rhosocial.stateflow.factory import AsyncOrderFactory, SyncOrderFactory
from rhosocial.stateflow.handlers import AsyncSubProcessHandler, SyncSubProcessHandler
from rhosocial.stateflow.models.flow_path import AsyncFlowPath, FlowPath
from rhosocial.stateflow.models.order import AsyncOrder, Order
from rhosocial.stateflow.models.order_event import AsyncOrderEvent, OrderEvent
from rhosocial.stateflow.models.order_outbox import AsyncOrderOutbox, OrderOutbox
from rhosocial.stateflow.models.order_process import AsyncOrderProcess, OrderProcess
from rhosocial.stateflow.models.order_subprocess import AsyncOrderSubProcess, OrderSubProcess
from rhosocial.stateflow.models.order_template import AsyncOrderTemplate, OrderTemplate
from rhosocial.stateflow.models.order_template_step import AsyncOrderTemplateStep, OrderTemplateStep
from rhosocial.stateflow.models.subprocess_dependency import (
    AsyncSubProcessDependency,
    SubProcessDependency,
)
from rhosocial.stateflow.registry import HandlerRegistry
from rhosocial.stateflow.types import SUBPROCESS_STATUS_RUNNING, HandlerResult

__all__ = ["AiAgentAssistant"]


_PLAN_DEF: Dict[str, Any] = {
    "name": "plan",
    "handler_class": "stateflow.applications.ai_agent.PlannerHandler",
    "terminal_states": ["planned", "plan_failed"],
    "advance_states": ["planned"],
    "rollback_states": ["plan_failed"],
    "step_order": 1,
}

# The dynamic tool chain (appended at runtime, not part of the template).
# Each tool is reversible and its success state is also rollback-eligible so
# upstream compensation can walk back in reverse order.
_TOOL_DEFS: List[Dict[str, Any]] = [
    {
        "name": "search",
        "handler_class": "stateflow.applications.ai_agent.SearchToolHandler",
        "terminal_states": ["searched", "search_failed"],
        "advance_states": ["searched"],
        "rollback_states": ["searched", "search_failed"],
        "is_reversible": True,
    },
    {
        "name": "write",
        "handler_class": "stateflow.applications.ai_agent.WriteToolHandler",
        "terminal_states": ["written", "write_failed"],
        "advance_states": ["written"],
        "rollback_states": ["written", "write_failed"],
        "is_reversible": True,
    },
    {
        "name": "review",
        "handler_class": "stateflow.applications.ai_agent.ReviewToolHandler",
        "terminal_states": ["reviewed", "review_failed"],
        "advance_states": ["reviewed"],
        "rollback_states": ["reviewed", "review_failed"],
        "is_reversible": True,
    },
]


class AiAgentAssistant:
    """Pre-built AI agent assistant with a runtime-defined execution graph.

    Workflow (dynamic)::

        plan ──append──▶ search → write → review
                              (runtime-defined tool chain)

    - ``build_template()`` creates only the seed ``plan`` step.
    - ``plan_steps_for(context)`` decides the tool chain at runtime.
    - ``run()`` appends the chain, advances ``plan`` (auto-starting the first
      tool), then drives each tool via its handler. A ``fail_step`` triggers
      the ``*_failed`` state and reverse-order compensation.

    A failure scenario::

        1. search → searched
        2. write → write_failed          (fail_step="write")
        3. publish_rollback(write)       → compensate write
        4. publish_rollback(search)      → compensate upstream search
    """

    name = "ai_agent_assistant"
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
        step = OrderTemplateStep(**_PLAN_DEF, template_id=template.id)
        return template, [step]

    @classmethod
    def build_async_template(cls) -> Tuple[AsyncOrderTemplate, list]:
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        step = AsyncOrderTemplateStep(**_PLAN_DEF, template_id=template.id)
        return template, [step]

    @classmethod
    def plan_steps_for(cls, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decide the tool chain at runtime from the task context.

        By default the full chain is used; ``quick=True`` drops the ``search``
        step, demonstrating that the graph is defined per-invocation.
        """
        if context.get("quick"):
            return [step for step in _TOOL_DEFS if step["name"] != "search"]
        return list(_TOOL_DEFS)

    @classmethod
    def sync_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        reg.register(_PLAN_DEF["handler_class"], PlannerHandler)
        for step in _TOOL_DEFS:
            reg.register(step["handler_class"], _SYNC_TOOL_HANDLERS[step["name"]])
        return reg

    @classmethod
    def async_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        reg.register(_PLAN_DEF["handler_class"], AsyncPlannerHandler)
        for step in _TOOL_DEFS:
            reg.register(step["handler_class"], _ASYNC_TOOL_HANDLERS[step["name"]])
        return reg

    # ------------------------------------------------------------------
    # Orchestrators (sync + async parity)
    # ------------------------------------------------------------------

    @classmethod
    def run(
        cls,
        service: Any,
        registry: HandlerRegistry,
        context: Dict[str, Any],
        *,
        fail_step: Optional[str] = None,
    ) -> Any:
        """Run the full dynamic agent flow synchronously; returns ``order_id``."""
        template, steps = cls.build_template()
        template.save()
        for step in steps:
            step.save()
        instance = SyncOrderFactory().create(template, steps, context=context)
        cls._persist(instance)
        order_id = instance.order.id

        cls._append_tool_chain(service, order_id, instance, context)

        plan_result = cls._start_plan(service, registry, order_id)
        if plan_result is None:
            return order_id

        for step in cls.plan_steps_for(context):
            cls._drive_step(
                service, registry, order_id, step["name"], fail_step=fail_step
            )

        if fail_step is not None:
            cls._compensate(service, order_id, fail_step, context)
        return order_id

    @classmethod
    async def run_async(
        cls,
        service: Any,
        registry: HandlerRegistry,
        context: Dict[str, Any],
        *,
        fail_step: Optional[str] = None,
    ) -> Any:
        """Run the full dynamic agent flow asynchronously; returns ``order_id``."""
        template, steps = cls.build_async_template()
        await template.save()
        for step in steps:
            await step.save()
        instance = await AsyncOrderFactory().create(template, steps, context=context)
        await cls._async_persist(instance)
        order_id = instance.order.id

        await cls._async_append_tool_chain(service, order_id, instance, context)

        plan_result = await cls._async_start_plan(service, registry, order_id)
        if plan_result is None:
            return order_id

        for step in cls.plan_steps_for(context):
            await cls._async_drive_step(
                service, registry, order_id, step["name"], fail_step=fail_step
            )

        if fail_step is not None:
            await cls._async_compensate(service, order_id, fail_step, context)
        return order_id

    # ------------------------------------------------------------------
    # Sync orchestration helpers
    # ------------------------------------------------------------------

    @classmethod
    def _append_tool_chain(cls, service, order_id, instance, context) -> None:
        """Append the tool chain at runtime, wiring ``depends_on`` edges."""
        previous = instance.get_subprocess(_PLAN_DEF["name"])
        for step in cls.plan_steps_for(context):
            previous = service.append_subprocess(
                order_id,
                name=step["name"],
                handler_class=step["handler_class"],
                terminal_states=step["terminal_states"],
                advance_states=step["advance_states"],
                rollback_states=step["rollback_states"],
                depends_on=[previous],
                is_reversible=step.get("is_reversible", False),
                event_key=f"ai-append-{order_id}-{step['name']}",
            )

    @classmethod
    def _start_plan(cls, service, registry, order_id):
        sp = cls._subprocess_for_step(order_id, _PLAN_DEF["name"])
        handler = registry.instantiate(sp.handler_class, sp)
        result = handler.start()
        if result is not None and result.status:
            service.publish_event(
                order_id=order_id,
                subprocess_id=sp.id,
                new_status=result.status,
                event_key=f"ai-plan-{order_id}",
            )
        return result

    @classmethod
    def _drive_step(cls, service, registry, order_id, name, *, fail_step) -> None:
        sp = cls._subprocess_for_step(order_id, name)
        if sp.status != SUBPROCESS_STATUS_RUNNING:
            # Dependency not satisfied — never auto-started; do not force-advance.
            return
        new_status: Optional[str]
        if fail_step == name:
            new_status = f"{name}_failed"
        else:
            handler = registry.instantiate(sp.handler_class, sp)
            result = handler.start()
            new_status = result.status if result is not None and result.status else None
        if new_status:
            service.publish_event(
                order_id=order_id,
                subprocess_id=sp.id,
                new_status=new_status,
                event_key=f"ai-{name}-{order_id}",
            )

    @classmethod
    def _compensate(cls, service, order_id, fail_step, context) -> None:
        """Compensate the failed step and its upstream tools in reverse order."""
        names = [step["name"] for step in cls.plan_steps_for(context)]
        index = names.index(fail_step)
        for name in reversed(names[: index + 1]):
            sp = cls._subprocess_for_step(order_id, name)
            service.publish_rollback(
                order_id=order_id,
                subprocess_id=sp.id,
                event_key=f"ai-rollback-{order_id}-{name}",
            )

    @staticmethod
    def _persist(instance) -> None:
        instance.order.save()
        instance.process.save()
        for sp in instance.subprocesses:
            sp.save()
        for dep in instance.dependencies:
            dep.save()
        for event in instance.events:
            event.save()

    @staticmethod
    def _subprocess_for_step(order_id, step_name):
        process = OrderProcess.query().where(OrderProcess.c.order_id == order_id).one()
        if process is None:
            raise ValueError(f"OrderProcess for order {order_id} not found")
        subprocess = (
            OrderSubProcess.query()
            .where(OrderSubProcess.c.process_id == process.id)
            .where(OrderSubProcess.c.step_name == step_name)
            .one()
        )
        if subprocess is None:
            raise ValueError(f"SubProcess '{step_name}' not found for order {order_id}")
        return subprocess

    # ------------------------------------------------------------------
    # Async orchestration helpers
    # ------------------------------------------------------------------

    @classmethod
    async def _async_append_tool_chain(cls, service, order_id, instance, context) -> None:
        previous = instance.get_subprocess(_PLAN_DEF["name"])
        for step in cls.plan_steps_for(context):
            previous = await service.append_subprocess(
                order_id,
                name=step["name"],
                handler_class=step["handler_class"],
                terminal_states=step["terminal_states"],
                advance_states=step["advance_states"],
                rollback_states=step["rollback_states"],
                depends_on=[previous],
                is_reversible=step.get("is_reversible", False),
                event_key=f"ai-append-{order_id}-{step['name']}",
            )

    @classmethod
    async def _async_start_plan(cls, service, registry, order_id):
        sp = await cls._async_subprocess_for_step(order_id, _PLAN_DEF["name"])
        handler = registry.instantiate_async(sp.handler_class, sp)
        result = await handler.start()
        if result is not None and result.status:
            await service.publish_event(
                order_id=order_id,
                subprocess_id=sp.id,
                new_status=result.status,
                event_key=f"ai-plan-{order_id}",
            )
        return result

    @classmethod
    async def _async_drive_step(cls, service, registry, order_id, name, *, fail_step) -> None:
        sp = await cls._async_subprocess_for_step(order_id, name)
        if sp.status != SUBPROCESS_STATUS_RUNNING:
            # Dependency not satisfied — never auto-started; do not force-advance.
            return
        new_status: Optional[str]
        if fail_step == name:
            new_status = f"{name}_failed"
        else:
            handler = registry.instantiate_async(sp.handler_class, sp)
            result = await handler.start()
            new_status = result.status if result is not None and result.status else None
        if new_status:
            await service.publish_event(
                order_id=order_id,
                subprocess_id=sp.id,
                new_status=new_status,
                event_key=f"ai-{name}-{order_id}",
            )

    @classmethod
    async def _async_compensate(cls, service, order_id, fail_step, context) -> None:
        names = [step["name"] for step in cls.plan_steps_for(context)]
        index = names.index(fail_step)
        for name in reversed(names[: index + 1]):
            sp = await cls._async_subprocess_for_step(order_id, name)
            await service.publish_rollback(
                order_id=order_id,
                subprocess_id=sp.id,
                event_key=f"ai-rollback-{order_id}-{name}",
            )

    @staticmethod
    async def _async_persist(instance) -> None:
        await instance.order.save()
        await instance.process.save()
        for sp in instance.subprocesses:
            await sp.save()
        for dep in instance.dependencies:
            await dep.save()
        for event in instance.events:
            await event.save()

    @staticmethod
    async def _async_subprocess_for_step(order_id, step_name):
        process = await AsyncOrderProcess.query().where(
            AsyncOrderProcess.c.order_id == order_id
        ).one()
        if process is None:
            raise ValueError(f"OrderProcess for order {order_id} not found")
        subprocess = await (
            AsyncOrderSubProcess.query()
            .where(AsyncOrderSubProcess.c.process_id == process.id)
            .where(AsyncOrderSubProcess.c.step_name == step_name)
            .one()
        )
        if subprocess is None:
            raise ValueError(f"SubProcess '{step_name}' not found for order {order_id}")
        return subprocess


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class PlannerHandler(SyncSubProcessHandler):
    """Decides the tool chain (the plan)."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="planned", event_key=f"ai-plan-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return None


class SearchToolHandler(SyncSubProcessHandler):
    """Simulated search tool call."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="searched", event_key=f"ai-search-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return None


class WriteToolHandler(SyncSubProcessHandler):
    """Simulated content writing tool call."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="written", event_key=f"ai-write-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return None


class ReviewToolHandler(SyncSubProcessHandler):
    """Simulated review tool call."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="reviewed", event_key=f"ai-review-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        return None


_SYNC_TOOL_HANDLERS: Dict[str, type] = {
    "search": SearchToolHandler,
    "write": WriteToolHandler,
    "review": ReviewToolHandler,
}


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncPlannerHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="planned", event_key=f"ai-plan-{self.subprocess.id}")

    async def rollback(self) -> Optional[HandlerResult]:
        return None


class AsyncSearchToolHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="searched", event_key=f"ai-search-{self.subprocess.id}")

    async def rollback(self) -> Optional[HandlerResult]:
        return None


class AsyncWriteToolHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="written", event_key=f"ai-write-{self.subprocess.id}")

    async def rollback(self) -> Optional[HandlerResult]:
        return None


class AsyncReviewToolHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="reviewed", event_key=f"ai-review-{self.subprocess.id}")

    async def rollback(self) -> Optional[HandlerResult]:
        return None


_ASYNC_TOOL_HANDLERS: Dict[str, type] = {
    "search": AsyncSearchToolHandler,
    "write": AsyncWriteToolHandler,
    "review": AsyncReviewToolHandler,
}
