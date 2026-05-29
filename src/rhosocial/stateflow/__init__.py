# src/rhosocial/stateflow/__init__.py
"""State transition and event-driven DAG orchestration for rhosocial."""

from .dispatcher import AsyncOrderDispatcher, SyncOrderDispatcher
from .exceptions import (
    ConcurrentStateTransitionError,
    DuplicateEventError,
    InvalidStateTransitionError,
    StateflowError,
    TemplateValidationError,
)
from .factory import AsyncOrderFactory, SyncOrderFactory
from .handlers import AsyncSubProcessHandler, SimulatedSubProcessHandler, SyncSubProcessHandler
from .models import (
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
from .types import HandlerResult, ValidationIssue, ValidationResult
from .validators import OrderTemplateValidator

__version__ = "1.0.0.dev1"

__all__ = [
    "AsyncOrderDispatcher",
    "AsyncOrderFactory",
    "AsyncSubProcessHandler",
    "ConcurrentStateTransitionError",
    "DuplicateEventError",
    "FlowPath",
    "HandlerResult",
    "InvalidStateTransitionError",
    "Order",
    "OrderEvent",
    "OrderOutbox",
    "OrderProcess",
    "OrderSubProcess",
    "OrderTemplate",
    "OrderTemplateStep",
    "OrderTemplateValidator",
    "SimulatedSubProcessHandler",
    "StateflowError",
    "SubProcessDependency",
    "SyncOrderDispatcher",
    "SyncOrderFactory",
    "SyncSubProcessHandler",
    "TemplateValidationError",
    "ValidationIssue",
    "ValidationResult",
]
