# src/rhosocial/stateflow/applications/ticket_system.py
"""Pre-built ticket system: create → assign → parallel (dev_fix + qa_verify) → close.

A branching workflow where two subprocesses run in parallel after assignment,
and the close step depends on both completing. Supports optional skip of
the QA step and dynamic append of a notification step at runtime.

Usage (sync):

```python
from rhosocial.stateflow import Schema, SyncOrderFactory, SyncOrderService
from rhosocial.stateflow.applications import TicketSystem

template, steps = TicketSystem.build_template()
template.save()
for step in steps: step.save()

instance = SyncOrderFactory().create(template, steps, context={"ticket_id": "T-100"})
# persist + use SyncOrderService with TicketSystem.sync_registry()
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

__all__ = ["TicketSystem"]


_TICKET_STEP_DEFS: list = [
    {
        "name": "create",
        "handler_class": "stateflow.applications.ticket_system.CreateHandler",
        "terminal_states": ["created"],
        "advance_states": ["created"],
        "step_order": 1,
    },
    {
        "name": "assign",
        "handler_class": "stateflow.applications.ticket_system.AssignHandler",
        "terminal_states": ["assigned"],
        "advance_states": ["assigned"],
        "depends_on": ["create"],
        "step_order": 2,
    },
    {
        "name": "dev_fix",
        "handler_class": "stateflow.applications.ticket_system.DevFixHandler",
        "terminal_states": ["fixed", "wont_fix"],
        "advance_states": ["fixed"],
        "depends_on": ["assign"],
        "step_order": 3,
    },
    {
        "name": "qa_verify",
        "handler_class": "stateflow.applications.ticket_system.QaVerifyHandler",
        "terminal_states": ["verified", "failed"],
        "advance_states": ["verified"],
        "depends_on": ["assign"],
        "step_order": 4,
    },
    {
        "name": "close",
        "handler_class": "stateflow.applications.ticket_system.CloseHandler",
        "terminal_states": ["closed"],
        "advance_states": ["closed"],
        "depends_on": ["dev_fix", "qa_verify"],
        "step_order": 5,
    },
]


class TicketSystem:
    """Pre-built ticket workflow component.

    Workflow::

        create → assign ─┬→ dev_fix  ─┐
                        └→ qa_verify ─┴→ close
    """

    name = "ticket_flow"
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
        """Create an in-memory ``OrderTemplate`` + steps (not persisted)."""
        template = OrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _TICKET_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps


    @classmethod
    def build_async_template(cls):
        """Create an in-memory async template + async steps (not persisted)."""
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _TICKET_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def sync_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _TICKET_STEP_DEFS:
            handler_cls = _SYNC_HANDLERS[defn["name"]]
            reg.register(defn["handler_class"], handler_cls)
        return reg

    @classmethod
    def async_registry(cls) -> HandlerRegistry:
        reg = HandlerRegistry()
        for defn in _TICKET_STEP_DEFS:
            handler_cls = _ASYNC_HANDLERS[defn["name"]]
            reg.register(defn["handler_class"], handler_cls)
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class CreateHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="created", event_key=f"create-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class AssignHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="assigned", event_key=f"assign-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class DevFixHandler(SyncSubProcessHandler):
    """Developer fix step.

    In a real system, ``start()`` would return ``None`` and the status
    would be set by an external signal (developer marks fixed/wont_fix).
    The auto-complete here is for demonstration.
    """
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="fixed", event_key=f"devfix-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class QaVerifyHandler(SyncSubProcessHandler):
    """QA verification step — same pattern as DevFixHandler."""
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verified", event_key=f"qaverify-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


class CloseHandler(SyncSubProcessHandler):
    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="closed", event_key=f"close-{self.subprocess.id}")
    def rollback(self) -> None:
        return None


_SYNC_HANDLERS = {
    "create": CreateHandler,
    "assign": AssignHandler,
    "dev_fix": DevFixHandler,
    "qa_verify": QaVerifyHandler,
    "close": CloseHandler,
}


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncCreateHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="created", event_key=f"create-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncAssignHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="assigned", event_key=f"assign-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncDevFixHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="fixed", event_key=f"devfix-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncQaVerifyHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="verified", event_key=f"qaverify-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


class AsyncCloseHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="closed", event_key=f"close-{self.subprocess.id}")
    async def rollback(self) -> None:
        return None


_ASYNC_HANDLERS = {
    "create": AsyncCreateHandler,
    "assign": AsyncAssignHandler,
    "dev_fix": AsyncDevFixHandler,
    "qa_verify": AsyncQaVerifyHandler,
    "close": AsyncCloseHandler,
}
