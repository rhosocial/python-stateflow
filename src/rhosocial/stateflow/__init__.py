# src/rhosocial/stateflow/__init__.py
"""State transition and event-driven DAG orchestration for rhosocial."""

from .deliverer import (
    AsyncOrderOutboxDeliverer,
    SyncOrderOutboxDeliverer,
    UnrecoverableDeliveryError,
)
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
from .registry import (
    AsyncHandlerRollbackTopic,
    AsyncHandlerStartTopic,
    HandlerRegistry,
    SyncHandlerRollbackTopic,
    SyncHandlerStartTopic,
    UnknownHandlerError,
)
from .schema import SQLITE_DDL, async_create_tables, async_drop_tables, create_tables, drop_tables
from .service import AsyncOrderService, SyncOrderService
from .timer import AsyncTimeoutScheduler, SyncTimeoutScheduler
from .types import HandlerResult, ValidationIssue, ValidationResult
from .validators import OrderTemplateValidator

__version__ = "1.0.0.dev1"

__all__ = [
    "AsyncFlowPath",
    "AsyncHandlerRollbackTopic",
    "AsyncHandlerStartTopic",
    "AsyncOrder",
    "AsyncOrderDispatcher",
    "AsyncOrderEvent",
    "AsyncOrderFactory",
    "AsyncOrderOutbox",
    "AsyncOrderOutboxDeliverer",
    "AsyncOrderProcess",
    "AsyncOrderService",
    "AsyncOrderSubProcess",
    "AsyncOrderTemplate",
    "AsyncOrderTemplateStep",
    "AsyncSubProcessDependency",
    "AsyncSubProcessHandler",
    "AsyncTimeoutScheduler",
    "ConcurrentStateTransitionError",
    "DuplicateEventError",
    "FlowPath",
    "HandlerRegistry",
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
    "SQLITE_DDL",
    "SimulatedSubProcessHandler",
    "StateflowError",
    "SubProcessDependency",
    "SyncHandlerRollbackTopic",
    "SyncHandlerStartTopic",
    "SyncOrderDispatcher",
    "SyncOrderFactory",
    "SyncOrderOutboxDeliverer",
    "SyncOrderService",
    "SyncSubProcessHandler",
    "SyncTimeoutScheduler",
    "TemplateValidationError",
    "UnknownHandlerError",
    "UnrecoverableDeliveryError",
    "ValidationIssue",
    "ValidationResult",
    "async_create_tables",
    "create_tables",
    "drop_tables",
    "async_drop_tables",
]
