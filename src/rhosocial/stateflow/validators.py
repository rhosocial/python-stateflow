# src/rhosocial/stateflow/validators.py
"""Template validation for stateflow."""

from typing import Dict, Iterable, List, Optional, Sequence, Set

from .models import FlowPath, OrderTemplateStep
from .types import ValidationResult


class OrderTemplateValidator:
    """Validates template DAG structure and state classification rules."""

    def validate(
        self,
        steps: Sequence[OrderTemplateStep],
        flow_paths: Optional[Sequence[FlowPath]] = None,
    ) -> ValidationResult:
        """Validate steps and optional flow paths without mutating the template."""
        result = ValidationResult()
        step_by_name = self._index_steps(steps, result)
        self._validate_state_sets(steps, result)
        self._validate_dependencies(steps, step_by_name, result)
        self._validate_acyclic(steps, step_by_name, result)
        self._validate_flow_paths(flow_paths or [], step_by_name, result)
        return result

    def _index_steps(
        self,
        steps: Sequence[OrderTemplateStep],
        result: ValidationResult,
    ) -> Dict[str, OrderTemplateStep]:
        step_by_name: Dict[str, OrderTemplateStep] = {}
        for index, step in enumerate(steps):
            if step.name in step_by_name:
                result.add(
                    "duplicate_step_name",
                    f"Duplicate step name: {step.name}",
                    f"steps[{index}].name",
                )
                continue
            step_by_name[step.name] = step
        return step_by_name

    def _validate_state_sets(
        self,
        steps: Sequence[OrderTemplateStep],
        result: ValidationResult,
    ) -> None:
        for index, step in enumerate(steps):
            terminal_states = set(step.terminal_states)
            advance_states = set(step.advance_states)
            rollback_states = set(step.rollback_states)

            missing_advance = advance_states - terminal_states
            if missing_advance:
                result.add(
                    "advance_state_not_terminal",
                    f"advance_states must be terminal states: {sorted(missing_advance)}",
                    f"steps[{index}].advance_states",
                )

            missing_rollback = rollback_states - terminal_states
            if missing_rollback:
                result.add(
                    "rollback_state_not_terminal",
                    f"rollback_states must be terminal states: {sorted(missing_rollback)}",
                    f"steps[{index}].rollback_states",
                )

            if step.timeout_seconds is not None and not step.timeout_status:
                result.add(
                    "timeout_status_required",
                    "timeout_status is required when timeout_seconds is configured",
                    f"steps[{index}].timeout_status",
                )

            if step.timeout_status and step.timeout_status not in terminal_states:
                result.add(
                    "timeout_status_not_terminal",
                    "timeout_status must be a terminal state",
                    f"steps[{index}].timeout_status",
                )

    def _validate_dependencies(
        self,
        steps: Sequence[OrderTemplateStep],
        step_by_name: Dict[str, OrderTemplateStep],
        result: ValidationResult,
    ) -> None:
        for index, step in enumerate(steps):
            for dependency in step.depends_on:
                if dependency not in step_by_name:
                    result.add(
                        "unknown_dependency",
                        f"Unknown dependency {dependency} for step {step.name}",
                        f"steps[{index}].depends_on",
                    )

    def _validate_acyclic(
        self,
        steps: Sequence[OrderTemplateStep],
        step_by_name: Dict[str, OrderTemplateStep],
        result: ValidationResult,
    ) -> None:
        graph = {
            step.name: [dependency for dependency in step.depends_on if dependency in step_by_name]
            for step in steps
        }
        visiting: Set[str] = set()
        visited: Set[str] = set()

        def visit(name: str, path: List[str]) -> None:
            if name in visited:
                return
            if name in visiting:
                cycle_start = path.index(name) if name in path else 0
                cycle = path[cycle_start:] + [name]
                result.add("cycle_detected", f"Cycle detected: {' -> '.join(cycle)}", "steps")
                return

            visiting.add(name)
            for dependency in graph.get(name, []):
                visit(dependency, path + [dependency])
            visiting.remove(name)
            visited.add(name)

        for step in steps:
            visit(step.name, [step.name])

    def _validate_flow_paths(
        self,
        flow_paths: Iterable[FlowPath],
        step_by_name: Dict[str, OrderTemplateStep],
        result: ValidationResult,
    ) -> None:
        for index, flow_path in enumerate(flow_paths):
            if flow_path.start_from and flow_path.start_from not in step_by_name:
                result.add(
                    "unknown_start_from",
                    f"Unknown start_from step: {flow_path.start_from}",
                    f"flow_paths[{index}].start_from",
                )

            for skipped_step in flow_path.skip_steps:
                if skipped_step not in step_by_name:
                    result.add(
                        "unknown_skip_step",
                        f"Unknown skipped step: {skipped_step}",
                        f"flow_paths[{index}].skip_steps",
                    )
