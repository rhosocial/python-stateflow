# src/rhosocial/stateflow/exceptions.py
"""Exceptions for stateflow."""


class StateflowError(Exception):
    """Base exception for all stateflow errors."""


class TemplateValidationError(StateflowError):
    """Raised when a template or dynamic subprocess definition is invalid."""


class InvalidStateTransitionError(StateflowError):
    """Raised when an event attempts an unsupported state transition."""


class DuplicateEventError(StateflowError):
    """Raised when a non-idempotent operation detects a duplicate event."""


class ConcurrentStateTransitionError(StateflowError):
    """Raised when optimistic concurrency rejects a state transition."""
