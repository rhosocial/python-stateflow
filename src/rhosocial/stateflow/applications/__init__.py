# src/rhosocial/stateflow/applications/__init__.py
"""Pre-built stateflow application components for common workflow patterns.

These modules are **production-ready building blocks** — not toy examples.
Each provides a pre-configured template, sync + async handler classes, and a
pre-registered ``HandlerRegistry``, so applications can get a full workflow
running with minimal boilerplate.

Each module defines:

1. A ``build_template()`` classmethod returning an ``OrderTemplate`` + steps.
2. Concrete ``SyncSubProcessHandler`` / ``AsyncSubProcessHandler`` implementations.
3. ``sync_registry()`` / ``async_registry()`` classmethods returning a
   pre-registered ``HandlerRegistry``.
4. ``models`` / ``async_models`` tuples for ``BackendGroup`` convenience.

Importing a module performs no I/O — the functions only build in-memory
objects. Callers configure backends and persist.

Sync and async paths are **fully parallel and non-interoperable**, following
the stateflow parity principle (see
``.claude/rules/sync-async-non-interoperability.md``).

Available components:

- :mod:`approval_flow` — content approval (submit → review → publish / reject)
- :mod:`ticket_system` — ticket workflow (create → assign → parallel dev+QA → close)
- :mod:`task_orchestration` — DAG task pipeline with timeout and rollback
- :mod:`agent_plan` — agent execution plan with failure compensation
- :mod:`ai_agent` — AI agent assistant with a runtime-defined execution graph
- :mod:`media_generation` — text-to-image/video generation order (freeze credits → generate → deliver / refund)
- :mod:`seat_booking` — fixed-seat ticketing (select → validate → pay → issue ticket)
- :mod:`external_services` — payment/credit protocols + mock implementations
"""

from .agent_plan import AgentPlan
from .ai_agent import AiAgentAssistant
from .approval_flow import ApprovalFlow
from .ticket_system import TicketSystem
from .task_orchestration import TaskOrchestration
from .media_generation import MediaGenerationFlow
from .seat_booking import SeatBookingFlow

__all__ = [
    "AgentPlan",
    "AiAgentAssistant",
    "ApprovalFlow",
    "MediaGenerationFlow",
    "SeatBookingFlow",
    "TaskOrchestration",
    "TicketSystem",
]
