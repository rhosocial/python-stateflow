# src/rhosocial/stateflow/applications/approval_flow.py
"""Pre-built approval flow: submit → review → publish / reject.

A linear three-step workflow where each step depends on the previous one.
The review step supports timeout (auto-reject) and rollback (reject →
compensate submit). Handlers are no-op stubs that report a fixed status;
override them by subclassing or replacing the registry entries.

Usage (sync):

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.stateflow import Schema, SyncOrderFactory, SyncOrderService
from rhosocial.stateflow.applications import ApprovalFlow

config = SQLiteConnectionConfig(database=":memory:")
with BackendGroup(name="stateflow", models=ApprovalFlow.models,
                  config=config, backend_class=SQLiteBackend) as group:
    backend = group.get_backend()
    backend.connect(); backend.introspect_and_adapt()
    Schema.create_tables(backend)

    template, steps = ApprovalFlow.build_template()
    template.save()
    for step in steps: step.save()

    instance = SyncOrderFactory().create(template, steps, context={"title": "RFC-001"})
    # persist + use SyncOrderService with ApprovalFlow.registry ...
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

__all__ = ["ApprovalFlow"]


# ---------------------------------------------------------------------------
# Step builder
# ---------------------------------------------------------------------------

_APPROVAL_STEP_DEFS: list = [
    {
        "name": "submit",
        "handler_class": "stateflow.applications.approval_flow.SubmitHandler",
        "terminal_states": ["submitted", "draft_error"],
        "advance_states": ["submitted"],
        "rollback_states": ["draft_error"],
        "step_order": 1,
    },
    {
        "name": "review",
        "handler_class": "stateflow.applications.approval_flow.ReviewHandler",
        "terminal_states": ["approved", "rejected", "timeout"],
        "advance_states": ["approved"],
        "rollback_states": ["rejected", "timeout"],
        "timeout_seconds": 86400,
        "timeout_status": "timeout",
        "depends_on": ["submit"],
        "step_order": 2,
    },
    {
        "name": "publish",
        "handler_class": "stateflow.applications.approval_flow.PublishHandler",
        "terminal_states": ["published", "publish_failed"],
        "advance_states": ["published"],
        "rollback_states": ["publish_failed"],
        "depends_on": ["review"],
        "step_order": 3,
    },
]


class ApprovalFlow:
    """Pre-built content approval flow component.

    Workflow::

        submit → review → publish
                  ↓
              rejected / timeout (rollback)
    """

    name = "content_approval"
    version = 1

    # All sync + async model classes, for BackendGroup convenience.
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

    # ------------------------------------------------------------------
    # Template + steps
    # ------------------------------------------------------------------

    @classmethod
    def build_template(cls) -> Tuple[OrderTemplate, list]:
        """Create an in-memory ``OrderTemplate`` + steps (not persisted)."""
        template = OrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _APPROVAL_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps


    @classmethod
    def build_async_template(cls):
        """Create an in-memory async AsyncOrderTemplate + async steps (not persisted)."""
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _APPROVAL_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    # ------------------------------------------------------------------
    # Handler registry
    # ------------------------------------------------------------------

    @classmethod
    def sync_registry(cls) -> HandlerRegistry:
        """Return a ``HandlerRegistry`` pre-registered with sync handlers."""
        reg = HandlerRegistry()
        reg.register(_APPROVAL_STEP_DEFS[0]["handler_class"], SubmitHandler)
        reg.register(_APPROVAL_STEP_DEFS[1]["handler_class"], ReviewHandler)
        reg.register(_APPROVAL_STEP_DEFS[2]["handler_class"], PublishHandler)
        return reg

    @classmethod
    def async_registry(cls) -> HandlerRegistry:
        """Return a ``HandlerRegistry`` pre-registered with async handlers."""
        reg = HandlerRegistry()
        reg.register(_APPROVAL_STEP_DEFS[0]["handler_class"], AsyncSubmitHandler)
        reg.register(_APPROVAL_STEP_DEFS[1]["handler_class"], AsyncReviewHandler)
        reg.register(_APPROVAL_STEP_DEFS[2]["handler_class"], AsyncPublishHandler)
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class SubmitHandler(SyncSubProcessHandler):
    """Auto-completes the submit step.

    Override ``start()`` to add validation or external API calls.
    """

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="submitted", event_key=f"submit-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class ReviewHandler(SyncSubProcessHandler):
    """Review step — does not auto-complete.

    In a real application, ``start()`` would notify a reviewer and return
    ``None`` (no status). The reviewer's decision arrives later as an
    external ``publish_event(new_status="approved"|"rejected")`` call.
    """

    def start(self) -> None:
        return None

    def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="rejected", event_key=f"review-rollback-{self.subprocess.id}")


class PublishHandler(SyncSubProcessHandler):
    """Auto-completes the publish step."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="published", event_key=f"publish-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Async handlers (mirror the sync ones)
# ---------------------------------------------------------------------------


class AsyncSubmitHandler(AsyncSubProcessHandler):
    """Async counterpart of :class:`SubmitHandler`."""

    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="submitted", event_key=f"submit-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncReviewHandler(AsyncSubProcessHandler):
    """Async counterpart of :class:`ReviewHandler`."""

    async def start(self) -> None:
        return None

    async def rollback(self) -> Optional[HandlerResult]:
        return HandlerResult(status="rejected", event_key=f"review-rollback-{self.subprocess.id}")


class AsyncPublishHandler(AsyncSubProcessHandler):
    """Async counterpart of :class:`PublishHandler`."""

    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="published", event_key=f"publish-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None
