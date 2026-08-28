# src/rhosocial/stateflow/factory.py
"""Factories for stateflow order instances.

Sync and async factories are **fully parallel and non-interoperable**: the
sync factory creates sync model instances (``Order``, ``OrderSubProcess``, …);
the async factory creates async model instances (``AsyncOrder``,
``AsyncOrderSubProcess``, …). The factory logic itself is pure (no I/O), but
the produced objects must match the caller's sync/async path so that
``save()`` is callable in the right mode.
"""

from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Sequence, Set

from .exceptions import TemplateValidationError
from .models import (
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderProcess,
    AsyncOrderSubProcess,
    AsyncSubProcessDependency,
    FlowPath,
    Order,
    OrderEvent,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
)
from .types import ORDER_STATUS_PENDING
from .validators import OrderTemplateValidator


@dataclass
class OrderInstance:
    """Complete runtime object graph produced by the factory.

    Holds model instances that may be sync or async depending on which factory
    produced them; the caller is expected to know which path they are on.
    """

    order: object
    process: Any
    subprocesses: List[Any]
    dependencies: List[Any]
    events: List[Any]

    def get_subprocess(self, step_name: str) -> Any:
        for subprocess in self.subprocesses:
            if subprocess.step_name == step_name:
                return subprocess
        raise KeyError(step_name)


class _FactoryBase:
    """Shared factory logic parameterised by model class references.

    Subclasses set the ``_order_cls``, ``_process_cls``, ``_subprocess_cls``,
    ``_dependency_cls`` and ``_event_cls`` class attributes to the appropriate
    sync or async model classes.
    """

    _order_cls: ClassVar[Any]
    _process_cls: ClassVar[Any]
    _subprocess_cls: ClassVar[Any]
    _dependency_cls: ClassVar[Any]
    _event_cls: ClassVar[Any]

    def __init__(self, validator: Optional[OrderTemplateValidator] = None):
        self.validator = validator or OrderTemplateValidator()

    def create(
        self,
        template: OrderTemplate,
        steps: Sequence[OrderTemplateStep],
        *,
        context: Optional[Dict] = None,
        flow_paths: Optional[Sequence[FlowPath]] = None,
        skip_steps: Optional[Iterable[str]] = None,
        start_from: Optional[str] = None,
    ) -> OrderInstance:
        """Create an order instance with snapshot, subprocesses, dependencies, and events."""
        validation = self.validator.validate(steps, flow_paths)
        if not validation.valid:
            messages = "; ".join(issue.message for issue in validation.issues)
            raise TemplateValidationError(messages)

        ordered_steps = template.ordered_steps(steps)
        skipped_names = set(skip_steps or [])
        if start_from:
            skipped_names.update(template.steps_before(ordered_steps, start_from))

        order = self._order_cls(
            template_id=template.id, status=ORDER_STATUS_PENDING, context=context or {}
        )
        process = self._process_cls.from_template(order, template, ordered_steps)
        subprocesses = self._build_subprocesses(process.id, ordered_steps, skipped_names)
        dependencies = self._build_dependencies(
            process.id, ordered_steps, subprocesses, skipped_names
        )
        events = self._build_initial_events(order, subprocesses)
        return OrderInstance(
            order=order,
            process=process,
            subprocesses=subprocesses,
            dependencies=dependencies,
            events=events,
        )

    def append_subprocess(
        self,
        process: Any,
        existing_subprocesses: Sequence[Any],
        existing_dependencies: Sequence[Any],
        *,
        name: str,
        handler_class: str,
        terminal_states: Sequence[str],
        advance_states: Sequence[str],
        rollback_states: Optional[Sequence[str]] = None,
        depends_on: Optional[Sequence[Any]] = None,
        timeout_seconds: Optional[int] = None,
        timeout_status: Optional[str] = None,
        is_reversible: bool = False,
    ) -> Any:
        """Append a dynamic subprocess; callers persist it and record append events."""
        if any(sp.step_name == name for sp in existing_subprocesses):
            raise TemplateValidationError(f"Duplicate subprocess name: {name}")
        dependency_names = {dep.step_name for dep in depends_on or []}
        if name in dependency_names:
            raise TemplateValidationError("A subprocess cannot depend on itself")

        return self._subprocess_cls.dynamic(
            process,
            existing_subprocesses,
            name=name,
            handler_class=handler_class,
            terminal_states=terminal_states,
            advance_states=advance_states,
            rollback_states=rollback_states,
            timeout_seconds=timeout_seconds,
            timeout_status=timeout_status,
            is_reversible=is_reversible,
        )

    def _build_subprocesses(
        self,
        process_id: object,
        steps: Sequence[OrderTemplateStep],
        skipped_names: Set[str],
    ) -> List[Any]:
        subprocesses: List[Any] = []
        for sequence, step in enumerate(steps):
            subprocesses.append(
                self._subprocess_cls.from_template_step(
                    process_id,
                    step,
                    step.name in skipped_names,
                    sequence,
                )
            )
        return subprocesses

    def _build_dependencies(
        self,
        process_id: object,
        steps: Sequence[OrderTemplateStep],
        subprocesses: Sequence[Any],
        skipped_names: Set[str],
    ) -> List[Any]:
        step_by_name = {step.name: step for step in steps}
        subprocess_by_name = {sp.step_name: sp for sp in subprocesses}
        dependencies: List[Any] = []
        for step in steps:
            if step.name in skipped_names:
                continue
            for dep_name in self._expanded_dependencies(step.name, step_by_name, skipped_names):
                dependencies.append(
                    self._dependency_cls.for_subprocess(
                        process_id,
                        subprocess_by_name[step.name],
                        subprocess_by_name[dep_name],
                    )
                )
        return dependencies

    def _expanded_dependencies(
        self,
        step_name: str,
        step_by_name: Dict[str, OrderTemplateStep],
        skipped_names: Set[str],
    ) -> Set[str]:
        expanded: Set[str] = set()
        for dep_name in step_by_name[step_name].depends_on:
            if dep_name in skipped_names:
                expanded.update(self._expanded_dependencies(dep_name, step_by_name, skipped_names))
            else:
                expanded.add(dep_name)
        return expanded

    def _build_initial_events(
        self,
        order: Any,
        subprocesses: Sequence[Any],
    ) -> List[Any]:
        events = [self._event_cls.order_created(order)]
        for sp in subprocesses:
            if sp.skipped:
                events.append(self._event_cls.subprocess_skipped(order, sp))
            else:
                events.append(self._event_cls.subprocess_created(order, sp))
        return events


class SyncOrderFactory(_FactoryBase):
    """Synchronous factory that creates sync model instances."""

    _order_cls = Order
    _process_cls = OrderProcess
    _subprocess_cls = OrderSubProcess
    _dependency_cls = SubProcessDependency
    _event_cls = OrderEvent


class AsyncOrderFactory(_FactoryBase):
    """Asynchronous factory that creates async model instances.

    The factory logic is pure (no I/O) so the ``async def`` methods do not
    ``await`` anything — they exist for API parity with
    :class:`SyncOrderFactory`. The async distinction matters at the
    persistence layer where ``await model.save()`` is called.
    """

    _order_cls = AsyncOrder
    _process_cls = AsyncOrderProcess
    _subprocess_cls = AsyncOrderSubProcess
    _dependency_cls = AsyncSubProcessDependency
    _event_cls = AsyncOrderEvent

    async def create(  # type: ignore[override]
        self,
        template: OrderTemplate,
        steps: Sequence[OrderTemplateStep],
        *,
        context: Optional[Dict] = None,
        flow_paths: Optional[Sequence[FlowPath]] = None,
        skip_steps: Optional[Iterable[str]] = None,
        start_from: Optional[str] = None,
    ) -> OrderInstance:
        """Create an order instance from a template asynchronously.

        Signature mirrors :meth:`_FactoryBase.create` exactly — only the
        ``async def`` nature differs. The async path passes async model
        instances which are structurally compatible.
        """
        return super().create(
            template=template,
            steps=steps,
            context=context,
            flow_paths=flow_paths,
            skip_steps=skip_steps,
            start_from=start_from,
        )

    async def append_subprocess(  # type: ignore[override]
        self,
        process: Any,
        existing_subprocesses: Sequence[Any],
        existing_dependencies: Sequence[Any],
        *,
        name: str,
        handler_class: str,
        terminal_states: Sequence[str],
        advance_states: Sequence[str],
        rollback_states: Optional[Sequence[str]] = None,
        depends_on: Optional[Sequence[Any]] = None,
        timeout_seconds: Optional[int] = None,
        timeout_status: Optional[str] = None,
        is_reversible: bool = False,
    ) -> Any:
        """Append a dynamic subprocess to an existing process asynchronously.

        Signature mirrors :meth:`_FactoryBase.append_subprocess` exactly.
        """
        return super().append_subprocess(
            process=process,
            existing_subprocesses=existing_subprocesses,
            existing_dependencies=existing_dependencies,
            name=name,
            handler_class=handler_class,
            terminal_states=terminal_states,
            advance_states=advance_states,
            rollback_states=rollback_states,
            depends_on=depends_on,
            timeout_seconds=timeout_seconds,
            timeout_status=timeout_status,
            is_reversible=is_reversible,
        )


__all__ = [
    "AsyncOrderFactory",
    "OrderInstance",
    "SyncOrderFactory",
]
