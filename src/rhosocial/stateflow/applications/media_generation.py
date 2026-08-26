# src/rhosocial/stateflow/applications/media_generation.py
"""Pre-built media generation order: text-to-image / text-to-video pipeline.

Workflow::

    collect_params → freeze_credits → submit_generation → poll_result → deliver → completed
                         ↓ (freeze failed)                    ↓ (gen failed)
                    order_failed                        refund_credits → order_failed

- ``collect_params``: gather prompt, model, size, etc. (auto-complete)
- ``freeze_credits``: pre-deduct credits via ``CreditService.freeze``;
  on failure the order is cancelled
- ``submit_generation``: send the task to the generation system;
  returns ``None`` (result arrives asynchronously)
- ``poll_result``: wait for the generation result; on timeout auto-fail;
  on success advance to deliver; on failure advance to refund
- ``deliver``: make the generated asset available to the user
- ``refund_credits``: unfreeze credits if generation failed

External services (``CreditService``) are injected via the handler's
``extra`` field or closure — they are NOT part of the stateflow core.
Use :class:`~rhosocial.stateflow.applications.external_services.MockCreditService`
for testing.
"""

from typing import Optional, Tuple

from ..handlers import AsyncSubProcessHandler, SyncSubProcessHandler
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
from ..registry import HandlerRegistry
from ..types import HandlerResult
from .external_services import (
    AsyncCreditService,
    CreditService,
    MockCreditService,
)

__all__ = ["MediaGenerationFlow"]

_MEDIA_STEP_DEFS: list = [
    {
        "name": "collect_params",
        "handler_class": "stateflow.applications.media_generation.CollectParamsHandler",
        "terminal_states": ["collected", "collect_failed"],
        "advance_states": ["collected"],
        "rollback_states": ["collect_failed"],
        "step_order": 1,
    },
    {
        "name": "freeze_credits",
        "handler_class": "stateflow.applications.media_generation.FreezeCreditsHandler",
        "terminal_states": ["frozen", "freeze_failed"],
        "advance_states": ["frozen"],
        "rollback_states": ["freeze_failed"],
        "depends_on": ["collect_params"],
        "step_order": 2,
    },
    {
        "name": "submit_generation",
        "handler_class": "stateflow.applications.media_generation.SubmitGenerationHandler",
        "terminal_states": ["submitted", "submit_failed"],
        "advance_states": ["submitted"],
        "rollback_states": ["submit_failed"],
        "timeout_seconds": 600,
        "timeout_status": "submit_failed",
        "depends_on": ["freeze_credits"],
        "step_order": 3,
    },
    {
        "name": "poll_result",
        "handler_class": "stateflow.applications.media_generation.PollResultHandler",
        "terminal_states": ["succeeded", "generation_failed", "timeout"],
        "advance_states": ["succeeded"],
        "rollback_states": ["generation_failed", "timeout"],
        "timeout_seconds": 1800,
        "timeout_status": "timeout",
        "depends_on": ["submit_generation"],
        "step_order": 4,
    },
    {
        "name": "deliver",
        "handler_class": "stateflow.applications.media_generation.DeliverHandler",
        "terminal_states": ["delivered", "deliver_failed"],
        "advance_states": ["delivered"],
        "rollback_states": ["deliver_failed"],
        "depends_on": ["poll_result"],
        "step_order": 5,
    },
    {
        "name": "refund_credits",
        "handler_class": "stateflow.applications.media_generation.RefundCreditsHandler",
        "terminal_states": ["refunded", "refund_failed"],
        "advance_states": ["refunded"],
        "rollback_states": ["refund_failed"],
        "depends_on": ["freeze_credits"],
        "step_order": 6,
    },
]


class MediaGenerationFlow:
    """Pre-built text-to-image / text-to-video generation order component.

    The flow supports two terminal paths:

    - **Success**: collect → freeze → submit → poll(succeeded) → deliver → completed
    - **Failure**: collect → freeze → submit → poll(failed) → refund → completed

    ``refund_credits`` is a side branch that depends on ``freeze_credits``
    (not ``poll_result``), so it can execute regardless of which downstream
    step failed. It only advances when ``poll_result`` reaches a rollback state.
    """

    name = "media_generation"
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
        for defn in _MEDIA_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def build_async_template(cls) -> Tuple[AsyncOrderTemplate, list]:
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _MEDIA_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def sync_registry(cls, credit_service: Optional[CreditService] = None) -> HandlerRegistry:
        """Return a pre-registered sync handler registry.

        Args:
            credit_service: concrete ``CreditService`` for freeze/refund.
                Defaults to :class:`MockCreditService` (in-memory, no real deduction).
        """
        cs = credit_service or MockCreditService()
        reg = HandlerRegistry()
        reg.register(_MEDIA_STEP_DEFS[0]["handler_class"], CollectParamsHandler)
        from functools import partial
        reg.register(_MEDIA_STEP_DEFS[1]["handler_class"], partial(FreezeCreditsHandler, credit_service=cs))
        reg.register(_MEDIA_STEP_DEFS[2]["handler_class"], SubmitGenerationHandler)
        reg.register(_MEDIA_STEP_DEFS[3]["handler_class"], PollResultHandler)
        reg.register(_MEDIA_STEP_DEFS[4]["handler_class"], DeliverHandler)
        reg.register(_MEDIA_STEP_DEFS[5]["handler_class"], partial(RefundCreditsHandler, credit_service=cs))
        return reg

    @classmethod
    def async_registry(cls, credit_service: Optional[AsyncCreditService] = None) -> HandlerRegistry:
        """Return a pre-registered async handler registry."""
        from .external_services import AsyncMockCreditService
        cs = credit_service or AsyncMockCreditService()
        reg = HandlerRegistry()
        reg.register(_MEDIA_STEP_DEFS[0]["handler_class"], AsyncCollectParamsHandler)
        from functools import partial
        reg.register(_MEDIA_STEP_DEFS[1]["handler_class"], partial(AsyncFreezeCreditsHandler, credit_service=cs))
        reg.register(_MEDIA_STEP_DEFS[2]["handler_class"], AsyncSubmitGenerationHandler)
        reg.register(_MEDIA_STEP_DEFS[3]["handler_class"], AsyncPollResultHandler)
        reg.register(_MEDIA_STEP_DEFS[4]["handler_class"], AsyncDeliverHandler)
        reg.register(_MEDIA_STEP_DEFS[5]["handler_class"], partial(AsyncRefundCreditsHandler, credit_service=cs))
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class CollectParamsHandler(SyncSubProcessHandler):
    """Validates generation parameters (prompt, model, size).

    In a real app this might validate against a model registry.
    """

    def start(self) -> Optional[HandlerResult]:
        prompt = self.subprocess.extra.get("prompt")
        if not prompt:
            return HandlerResult(status="collect_failed", event_key=f"collect-{self.subprocess.id}")
        return HandlerResult(status="collected", event_key=f"collect-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class FreezeCreditsHandler(SyncSubProcessHandler):
    """Freezes credits via ``CreditService.freeze``.

    The handler is constructed with a concrete ``CreditService`` instance.
    The freeze id is stored in ``subprocess.extra["freeze_id"]`` for later
    use by :class:`RefundCreditsHandler`.
    """

    def __init__(self, subprocess, credit_service: CreditService):
        super().__init__(subprocess)
        self.credit_service = credit_service

    def start(self) -> Optional[HandlerResult]:
        user_id = self.subprocess.extra.get("user_id", "")
        amount = self.subprocess.extra.get("credit_cost", 0)
        try:
            freeze_id = self.credit_service.freeze(user_id, amount, reason=f"media-gen-{self.subprocess.process_id}")
            self.subprocess.extra["freeze_id"] = freeze_id
            self.subprocess.save()
            return HandlerResult(status="frozen", event_key=f"freeze-{self.subprocess.id}")
        except Exception:
            return HandlerResult(status="freeze_failed", event_key=f"freeze-fail-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        freeze_id = self.subprocess.extra.get("freeze_id")
        if freeze_id:
            self.credit_service.unfreeze(freeze_id)
        return None


class SubmitGenerationHandler(SyncSubProcessHandler):
    """Submits the generation task to the generation system.

    In a real app this calls an external API. Here it just records the task id.
    """

    def start(self) -> Optional[HandlerResult]:
        import uuid as _uuid
        task_id = str(_uuid.uuid4())
        self.subprocess.extra["gen_task_id"] = task_id
        self.subprocess.save()
        return HandlerResult(status="submitted", event_key=f"submit-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class PollResultHandler(SyncSubProcessHandler):
    """Polls the generation result.

    In a real app this returns ``None`` (waits for callback). Here it
    auto-completes based on ``subprocess.extra["simulate_success"]``.
    """

    def start(self) -> Optional[HandlerResult]:
        if self.subprocess.extra.get("simulate_success", True):
            self.subprocess.extra["asset_url"] = "https://cdn.example.com/generated.png"
            self.subprocess.save()
            return HandlerResult(status="succeeded", event_key=f"poll-{self.subprocess.id}")
        return HandlerResult(status="generation_failed", event_key=f"poll-fail-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class DeliverHandler(SyncSubProcessHandler):
    """Delivers the generated asset to the user."""

    def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="delivered", event_key=f"deliver-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class RefundCreditsHandler(SyncSubProcessHandler):
    """Unfreezes credits when generation fails.

    Uses the ``freeze_id`` stored by :class:`FreezeCreditsHandler`.
    """

    def __init__(self, subprocess, credit_service: CreditService):
        super().__init__(subprocess)
        self.credit_service = credit_service

    def start(self) -> Optional[HandlerResult]:
        freeze_id = self.subprocess.extra.get("freeze_id")
        if freeze_id and self.credit_service.unfreeze(freeze_id):
            return HandlerResult(status="refunded", event_key=f"refund-{self.subprocess.id}")
        return HandlerResult(status="refund_failed", event_key=f"refund-fail-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncCollectParamsHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        prompt = self.subprocess.extra.get("prompt")
        if not prompt:
            return HandlerResult(status="collect_failed", event_key=f"collect-{self.subprocess.id}")
        return HandlerResult(status="collected", event_key=f"collect-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncFreezeCreditsHandler(AsyncSubProcessHandler):
    def __init__(self, subprocess, credit_service: AsyncCreditService):
        super().__init__(subprocess)
        self.credit_service = credit_service

    async def start(self) -> Optional[HandlerResult]:
        user_id = self.subprocess.extra.get("user_id", "")
        amount = self.subprocess.extra.get("credit_cost", 0)
        try:
            freeze_id = await self.credit_service.freeze(user_id, amount, reason=f"media-gen-{self.subprocess.process_id}")
            self.subprocess.extra["freeze_id"] = freeze_id
            await self.subprocess.save()
            return HandlerResult(status="frozen", event_key=f"freeze-{self.subprocess.id}")
        except Exception:
            return HandlerResult(status="freeze_failed", event_key=f"freeze-fail-{self.subprocess.id}")

    async def rollback(self) -> None:
        freeze_id = self.subprocess.extra.get("freeze_id")
        if freeze_id:
            await self.credit_service.unfreeze(freeze_id)
        return None


class AsyncSubmitGenerationHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        import uuid as _uuid
        task_id = str(_uuid.uuid4())
        self.subprocess.extra["gen_task_id"] = task_id
        await self.subprocess.save()
        return HandlerResult(status="submitted", event_key=f"submit-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncPollResultHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        if self.subprocess.extra.get("simulate_success", True):
            self.subprocess.extra["asset_url"] = "https://cdn.example.com/generated.png"
            await self.subprocess.save()
            return HandlerResult(status="succeeded", event_key=f"poll-{self.subprocess.id}")
        return HandlerResult(status="generation_failed", event_key=f"poll-fail-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncDeliverHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        return HandlerResult(status="delivered", event_key=f"deliver-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncRefundCreditsHandler(AsyncSubProcessHandler):
    def __init__(self, subprocess, credit_service: AsyncCreditService):
        super().__init__(subprocess)
        self.credit_service = credit_service

    async def start(self) -> Optional[HandlerResult]:
        freeze_id = self.subprocess.extra.get("freeze_id")
        if freeze_id and await self.credit_service.unfreeze(freeze_id):
            return HandlerResult(status="refunded", event_key=f"refund-{self.subprocess.id}")
        return HandlerResult(status="refund_failed", event_key=f"refund-fail-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None
