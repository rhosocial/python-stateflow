# src/rhosocial/stateflow/factory.py
"""Factories for stateflow order instances."""

import uuid
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

from .exceptions import TemplateValidationError
from .models import (
    FlowPath,
    Order,
    OrderEvent,
    OrderProcess,
    OrderSubProcess,
    OrderTemplate,
    OrderTemplateStep,
    SubProcessDependency,
)
from .types import (
    EVENT_ORDER_CREATED,
    EVENT_SP_CREATED,
    EVENT_SP_SKIPPED,
    ORDER_STATUS_PENDING,
    SUBPROCESS_SOURCE_DYNAMIC,
    SUBPROCESS_SOURCE_TEMPLATE,
    SUBPROCESS_STATUS_PENDING,
)
from .validators import OrderTemplateValidator


@dataclass
class OrderInstance:
    """Complete runtime object graph produced by the factory."""

    order: Order
    process: OrderProcess
    subprocesses: List[OrderSubProcess]
    dependencies: List[SubProcessDependency]
    events: List[OrderEvent]

    def get_subprocess(self, step_name: str) -> OrderSubProcess:
        for subprocess in self.subprocesses:
            if subprocess.step_name == step_name:
                return subprocess
        raise KeyError(step_name)


class SyncOrderFactory:
    """Synchronous factory that turns templates into runtime order objects."""

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

        ordered_steps = sorted(steps, key=lambda step: step.step_order)
        skipped_names = set(skip_steps or [])
        if start_from:
            skipped_names.update(self._steps_before(ordered_steps, start_from))

        order = Order(template_id=template.id, status=ORDER_STATUS_PENDING, context=context or {})
        process = OrderProcess(order_id=order.id, template_snapshot=self._snapshot(template, ordered_steps))
        subprocesses = self._build_subprocesses(process.id, ordered_steps, skipped_names)
        dependencies = self._build_dependencies(process.id, ordered_steps, subprocesses, skipped_names)
        events = self._build_initial_events(order.id, subprocesses)
        return OrderInstance(
            order=order,
            process=process,
            subprocesses=subprocesses,
            dependencies=dependencies,
            events=events,
        )

    def append_subprocess(
        self,
        process: OrderProcess,
        existing_subprocesses: Sequence[OrderSubProcess],
        existing_dependencies: Sequence[SubProcessDependency],
        *,
        name: str,
        handler_class: str,
        terminal_states: Sequence[str],
        advance_states: Sequence[str],
        rollback_states: Optional[Sequence[str]] = None,
        depends_on: Optional[Sequence[OrderSubProcess]] = None,
        timeout_seconds: Optional[int] = None,
        timeout_status: Optional[str] = None,
        is_reversible: bool = False,
    ) -> OrderSubProcess:
        """Append a dynamic subprocess; callers persist it and record append events."""
        if any(subprocess.step_name == name for subprocess in existing_subprocesses):
            raise TemplateValidationError(f"Duplicate subprocess name: {name}")
        dependency_names = {dependency.step_name for dependency in depends_on or []}
        if name in dependency_names:
            raise TemplateValidationError("A subprocess cannot depend on itself")

        sequence = max((subprocess.sequence for subprocess in existing_subprocesses), default=-1) + 1
        return OrderSubProcess(
            process_id=process.id,
            step_name=name,
            status=SUBPROCESS_STATUS_PENDING,
            handler_class=handler_class,
            terminal_states=list(terminal_states),
            advance_states=list(advance_states),
            rollback_states=list(rollback_states or []),
            timeout_seconds=timeout_seconds,
            timeout_status=timeout_status,
            source=SUBPROCESS_SOURCE_DYNAMIC,
            sequence=sequence,
            is_reversible=is_reversible,
        )

    def _steps_before(self, steps: Sequence[OrderTemplateStep], start_from: str) -> Set[str]:
        before: Set[str] = set()
        for step in steps:
            if step.name == start_from:
                return before
            before.add(step.name)
        raise TemplateValidationError(f"Unknown start_from step: {start_from}")

    def _snapshot(self, template: OrderTemplate, steps: Sequence[OrderTemplateStep]) -> Dict:
        return {
            "template": {
                "id": str(template.id),
                "name": template.name,
                "version": template.version,
                "status": template.status,
            },
            "steps": [
                {
                    "name": step.name,
                    "handler_class": step.handler_class,
                    "terminal_states": list(step.terminal_states),
                    "advance_states": list(step.advance_states),
                    "rollback_states": list(step.rollback_states),
                    "timeout_seconds": step.timeout_seconds,
                    "timeout_status": step.timeout_status,
                    "depends_on": list(step.depends_on),
                    "step_order": step.step_order,
                }
                for step in steps
            ],
        }

    def _build_subprocesses(
        self,
        process_id: uuid.UUID,
        steps: Sequence[OrderTemplateStep],
        skipped_names: Set[str],
    ) -> List[OrderSubProcess]:
        subprocesses: List[OrderSubProcess] = []
        for sequence, step in enumerate(steps):
            subprocesses.append(
                OrderSubProcess(
                    process_id=process_id,
                    step_name=step.name,
                    status=SUBPROCESS_STATUS_PENDING,
                    handler_class=step.handler_class,
                    terminal_states=list(step.terminal_states),
                    advance_states=list(step.advance_states),
                    rollback_states=list(step.rollback_states),
                    timeout_seconds=step.timeout_seconds,
                    timeout_status=step.timeout_status,
                    skipped=step.name in skipped_names,
                    source=SUBPROCESS_SOURCE_TEMPLATE,
                    sequence=sequence,
                )
            )
        return subprocesses

    def _build_dependencies(
        self,
        process_id: uuid.UUID,
        steps: Sequence[OrderTemplateStep],
        subprocesses: Sequence[OrderSubProcess],
        skipped_names: Set[str],
    ) -> List[SubProcessDependency]:
        step_by_name = {step.name: step for step in steps}
        subprocess_by_name = {subprocess.step_name: subprocess for subprocess in subprocesses}
        dependencies: List[SubProcessDependency] = []
        for step in steps:
            if step.name in skipped_names:
                continue
            for dependency_name in self._expanded_dependencies(step.name, step_by_name, skipped_names):
                dependencies.append(
                    SubProcessDependency(
                        process_id=process_id,
                        subprocess_id=subprocess_by_name[step.name].id,
                        depends_on_id=subprocess_by_name[dependency_name].id,
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
        for dependency_name in step_by_name[step_name].depends_on:
            if dependency_name in skipped_names:
                expanded.update(self._expanded_dependencies(dependency_name, step_by_name, skipped_names))
            else:
                expanded.add(dependency_name)
        return expanded

    def _build_initial_events(
        self,
        order_id: uuid.UUID,
        subprocesses: Sequence[OrderSubProcess],
    ) -> List[OrderEvent]:
        events = [OrderEvent(order_id=order_id, event_type=EVENT_ORDER_CREATED)]
        for subprocess in subprocesses:
            events.append(
                OrderEvent(
                    order_id=order_id,
                    subprocess_id=subprocess.id,
                    event_type=EVENT_SP_SKIPPED if subprocess.skipped else EVENT_SP_CREATED,
                    to_status=subprocess.status,
                    payload={"step_name": subprocess.step_name},
                )
            )
        return events


class AsyncOrderFactory:
    def __init__(self, validator: Optional[OrderTemplateValidator] = None):
        self._sync_factory = SyncOrderFactory(validator)

    async def create(self, *args, **kwargs) -> OrderInstance:
        """Create an order instance from a template asynchronously."""
        return self._sync_factory.create(*args, **kwargs)

    async def append_subprocess(self, *args, **kwargs) -> OrderSubProcess:
        """Append a dynamic subprocess to an existing process asynchronously."""
        return self._sync_factory.append_subprocess(*args, **kwargs)
