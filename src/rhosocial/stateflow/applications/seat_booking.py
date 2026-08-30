# src/rhosocial/stateflow/applications/seat_booking.py
"""Pre-built fixed-seat booking: seat selection → validation → payment → ticketing.

Workflow::

    select_seat → validate → payment → issue_ticket → completed
                      ↓ (invalid)        ↓ (payment failed)
                  seat_invalid      seat_released → booking_failed

- ``select_seat``: user selects a seat from a pre-generated inventory.
  The seat id is stored in ``subprocess.extra["seat_id"]``.
- ``validate``: checks seat availability, overlapping bookings, etc.
- ``payment``: charges via ``PaymentService``; on failure the seat is released.
- ``issue_ticket``: generates the ticket / confirmation.

External services (``PaymentService``) are injected via the handler
constructor. Use :class:`~rhosocial.stateflow.applications.external_services.MockPaymentService`
for testing.
"""

from typing import Optional, Tuple

from ..handlers import AsyncSubProcessHandler, SyncSubProcessHandler
from rhosocial.stateflow.models.flow_path import AsyncFlowPath, FlowPath
from rhosocial.stateflow.models.order import AsyncOrder, Order
from rhosocial.stateflow.models.order_event import AsyncOrderEvent, OrderEvent
from rhosocial.stateflow.models.order_outbox import AsyncOrderOutbox, OrderOutbox
from rhosocial.stateflow.models.order_process import AsyncOrderProcess, OrderProcess
from rhosocial.stateflow.models.order_subprocess import AsyncOrderSubProcess, OrderSubProcess
from rhosocial.stateflow.models.order_template import AsyncOrderTemplate, OrderTemplate
from rhosocial.stateflow.models.order_template_step import AsyncOrderTemplateStep, OrderTemplateStep
from rhosocial.stateflow.models.subprocess_dependency import AsyncSubProcessDependency, SubProcessDependency
from rhosocial.stateflow.registry import HandlerRegistry
from ..types import HandlerResult
from .external_services import (
    AsyncPaymentService,
    MockPaymentService,
    PaymentService,
)

__all__ = ["SeatBookingFlow"]

_SEAT_STEP_DEFS: list = [
    {
        "name": "select_seat",
        "handler_class": "stateflow.applications.seat_booking.SelectSeatHandler",
        "terminal_states": ["seat_selected", "seat_unavailable"],
        "advance_states": ["seat_selected"],
        "rollback_states": ["seat_unavailable"],
        "step_order": 1,
    },
    {
        "name": "validate",
        "handler_class": "stateflow.applications.seat_booking.ValidateHandler",
        "terminal_states": ["validated", "validation_failed"],
        "advance_states": ["validated"],
        "rollback_states": ["validation_failed"],
        "depends_on": ["select_seat"],
        "step_order": 2,
    },
    {
        "name": "payment",
        "handler_class": "stateflow.applications.seat_booking.PaymentHandler",
        "terminal_states": ["paid", "payment_failed", "timeout"],
        "advance_states": ["paid"],
        "rollback_states": ["payment_failed", "timeout"],
        "timeout_seconds": 900,
        "timeout_status": "timeout",
        "depends_on": ["validate"],
        "step_order": 3,
    },
    {
        "name": "issue_ticket",
        "handler_class": "stateflow.applications.seat_booking.IssueTicketHandler",
        "terminal_states": ["ticketed", "ticket_failed"],
        "advance_states": ["ticketed"],
        "rollback_states": ["ticket_failed"],
        "depends_on": ["payment"],
        "step_order": 4,
    },
]


class SeatBookingFlow:
    """Pre-built fixed-seat ticketing component.

    Supports pre-generated seat inventory with validation, payment,
    and ticket issuance. Payment is delegated to an external
    :class:`PaymentService`.

    Failure paths:
    - ``select_seat`` → ``seat_unavailable``: seat already taken
    - ``validate`` → ``validation_failed``: invalid request
    - ``payment`` → ``payment_failed`` / ``timeout``: seat released
    """

    name = "seat_booking"
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
        for defn in _SEAT_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(OrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def build_async_template(cls) -> Tuple[AsyncOrderTemplate, list]:
        template = AsyncOrderTemplate(name=cls.name, version=cls.version)
        steps = []
        for defn in _SEAT_STEP_DEFS:
            kwargs = {**defn, "template_id": template.id}
            steps.append(AsyncOrderTemplateStep(**kwargs))
        return template, steps

    @classmethod
    def sync_registry(cls, payment_service: Optional[PaymentService] = None) -> HandlerRegistry:
        """Return a pre-registered sync handler registry.

        Args:
            payment_service: concrete ``PaymentService`` for charging.
                Defaults to :class:`MockPaymentService`.
        """
        ps = payment_service or MockPaymentService()
        reg = HandlerRegistry()
        reg.register(_SEAT_STEP_DEFS[0]["handler_class"], SelectSeatHandler)
        reg.register(_SEAT_STEP_DEFS[1]["handler_class"], ValidateHandler)
        from functools import partial
        reg.register(_SEAT_STEP_DEFS[2]["handler_class"], partial(PaymentHandler, payment_service=ps))
        reg.register(_SEAT_STEP_DEFS[3]["handler_class"], IssueTicketHandler)
        return reg

    @classmethod
    def async_registry(cls, payment_service: Optional[AsyncPaymentService] = None) -> HandlerRegistry:
        """Return a pre-registered async handler registry."""
        from .external_services import AsyncMockPaymentService
        ps = payment_service or AsyncMockPaymentService()
        reg = HandlerRegistry()
        reg.register(_SEAT_STEP_DEFS[0]["handler_class"], AsyncSelectSeatHandler)
        reg.register(_SEAT_STEP_DEFS[1]["handler_class"], AsyncValidateHandler)
        from functools import partial
        reg.register(_SEAT_STEP_DEFS[2]["handler_class"], partial(AsyncPaymentHandler, payment_service=ps))
        reg.register(_SEAT_STEP_DEFS[3]["handler_class"], AsyncIssueTicketHandler)
        return reg


# ---------------------------------------------------------------------------
# Sync handlers
# ---------------------------------------------------------------------------


class SelectSeatHandler(SyncSubProcessHandler):
    """Records the selected seat.

    In a real system this would check a seat inventory service. Here it
    accepts any seat id present in ``subprocess.extra["seat_id"]``.
    """

    def start(self) -> Optional[HandlerResult]:
        seat_id = self.subprocess.extra.get("seat_id")
        if not seat_id:
            return HandlerResult(status="seat_unavailable", event_key=f"seat-fail-{self.subprocess.id}")
        return HandlerResult(status="seat_selected", event_key=f"seat-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class ValidateHandler(SyncSubProcessHandler):
    """Validates the booking request (seat exists, no overlap, etc.).

    In a real system this would query an inventory database. Here it
    accepts everything that has a valid ``seat_id``.
    """

    def start(self) -> Optional[HandlerResult]:
        seat_id = self.subprocess.extra.get("seat_id")
        if seat_id:
            return HandlerResult(status="validated", event_key=f"validate-{self.subprocess.id}")
        return HandlerResult(status="validation_failed", event_key=f"validate-fail-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


class PaymentHandler(SyncSubProcessHandler):
    """Charges the user via ``PaymentService``.

    The transaction id is stored in ``subprocess.extra["tx_id"]`` for refunds.
    """

    def __init__(self, subprocess, payment_service: PaymentService):
        super().__init__(subprocess)
        self.payment_service = payment_service

    def start(self) -> Optional[HandlerResult]:
        order_id = self.subprocess.process_id
        amount = self.subprocess.extra.get("price", 0)
        tx_id = self.payment_service.charge(order_id, amount)
        status = self.payment_service.get_status(tx_id)
        self.subprocess.extra["tx_id"] = tx_id
        self.subprocess.save()
        if status == "succeeded":
            return HandlerResult(status="paid", event_key=f"pay-{self.subprocess.id}")
        return HandlerResult(status="payment_failed", event_key=f"pay-fail-{self.subprocess.id}")

    def rollback(self) -> Optional[HandlerResult]:
        tx_id = self.subprocess.extra.get("tx_id")
        if tx_id:
            self.payment_service.refund(tx_id)
        return None


class IssueTicketHandler(SyncSubProcessHandler):
    """Generates the ticket / confirmation number."""

    def start(self) -> Optional[HandlerResult]:
        import uuid as _uuid
        ticket_no = f"TKT-{_uuid.uuid4().hex[:8].upper()}"
        self.subprocess.extra["ticket_no"] = ticket_no
        self.subprocess.save()
        return HandlerResult(status="ticketed", event_key=f"ticket-{self.subprocess.id}")

    def rollback(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Async handlers
# ---------------------------------------------------------------------------


class AsyncSelectSeatHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        seat_id = self.subprocess.extra.get("seat_id")
        if not seat_id:
            return HandlerResult(status="seat_unavailable", event_key=f"seat-fail-{self.subprocess.id}")
        return HandlerResult(status="seat_selected", event_key=f"seat-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncValidateHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        seat_id = self.subprocess.extra.get("seat_id")
        if seat_id:
            return HandlerResult(status="validated", event_key=f"validate-{self.subprocess.id}")
        return HandlerResult(status="validation_failed", event_key=f"validate-fail-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None


class AsyncPaymentHandler(AsyncSubProcessHandler):
    def __init__(self, subprocess, payment_service: AsyncPaymentService):
        super().__init__(subprocess)
        self.payment_service = payment_service

    async def start(self) -> Optional[HandlerResult]:
        order_id = self.subprocess.process_id
        amount = self.subprocess.extra.get("price", 0)
        tx_id = await self.payment_service.charge(order_id, amount)
        status = await self.payment_service.get_status(tx_id)
        self.subprocess.extra["tx_id"] = tx_id
        await self.subprocess.save()
        if status == "succeeded":
            return HandlerResult(status="paid", event_key=f"pay-{self.subprocess.id}")
        return HandlerResult(status="payment_failed", event_key=f"pay-fail-{self.subprocess.id}")

    async def rollback(self) -> None:
        tx_id = self.subprocess.extra.get("tx_id")
        if tx_id:
            await self.payment_service.refund(tx_id)
        return None


class AsyncIssueTicketHandler(AsyncSubProcessHandler):
    async def start(self) -> Optional[HandlerResult]:
        import uuid as _uuid
        ticket_no = f"TKT-{_uuid.uuid4().hex[:8].upper()}"
        self.subprocess.extra["ticket_no"] = ticket_no
        await self.subprocess.save()
        return HandlerResult(status="ticketed", event_key=f"ticket-{self.subprocess.id}")

    async def rollback(self) -> None:
        return None
