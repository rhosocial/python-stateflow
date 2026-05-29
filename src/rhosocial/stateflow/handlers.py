# src/rhosocial/stateflow/handlers.py
"""Handler interfaces for stateflow subprocesses."""

from abc import ABC, abstractmethod
from typing import Optional

from .types import HandlerResult


class SyncSubProcessHandler(ABC):
    """Synchronous handler contract for subprocess side effects."""

    def __init__(self, subprocess):
        self.subprocess = subprocess

    @abstractmethod
    def start(self) -> Optional[HandlerResult]:
        """Start the subprocess."""
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> Optional[HandlerResult]:
        """Rollback the subprocess."""
        raise NotImplementedError


class AsyncSubProcessHandler(ABC):
    """Asynchronous handler contract for subprocess side effects."""

    def __init__(self, subprocess):
        self.subprocess = subprocess

    @abstractmethod
    async def start(self) -> Optional[HandlerResult]:
        """Start the subprocess asynchronously."""
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> Optional[HandlerResult]:
        """Rollback the subprocess asynchronously."""
        raise NotImplementedError


class SimulatedSubProcessHandler(SyncSubProcessHandler):
    """Synchronous test handler returning a configured status without side effects."""

    simulate_status: str

    def start(self) -> HandlerResult:
        """Start the subprocess with a simulated result."""
        return HandlerResult(status=self.simulate_status)

    def rollback(self) -> None:
        """Rollback the simulated subprocess."""
        return None
