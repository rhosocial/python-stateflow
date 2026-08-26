# tests/providers/base.py
"""Provider protocols for stateflow multi-backend tests.

A provider encapsulates everything a test needs to run against a specific
backend: model configuration, schema creation, and teardown. Sync and async
providers are separate (non-interoperable) implementations, mirroring the
stateflow sync/async architecture.
"""

from abc import ABC, abstractmethod
from typing import Sequence, Type


class StateflowSyncProvider(ABC):
    """Protocol for sync backend providers.

    Implementations configure all stateflow sync models on a single shared
    backend, create the schema, and expose the model classes for test use.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable backend name (e.g. ``"sqlite-sync"``)."""

    @abstractmethod
    def setup(self) -> object:
        """Configure models, create schema, return a backend group handle."""

    @abstractmethod
    def teardown(self, handle: object) -> None:
        """Drop schema and disconnect."""

    @property
    @abstractmethod
    def models(self) -> Sequence[Type]:
        """All sync model classes configured by this provider."""


class StateflowAsyncProvider(ABC):
    """Protocol for async backend providers.

    Implementations configure all stateflow async models on a single shared
    async backend, create the schema, and expose the model classes for test use.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable backend name (e.g. ``"sqlite-async"``)."""

    @abstractmethod
    async def setup(self) -> object:
        """Configure models, create schema, return a backend group handle."""

    @abstractmethod
    async def teardown(self, handle: object) -> None:
        """Drop schema and disconnect."""

    @property
    @abstractmethod
    def models(self) -> Sequence[Type]:
        """All async model classes configured by this provider."""
