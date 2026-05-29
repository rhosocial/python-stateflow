# Core Concepts

## Template layer

- `OrderTemplate`: process blueprint.
- `OrderTemplateStep`: subprocess definition and state rules.
- `FlowPath`: optional path variant.

## Instance layer

- `Order`: runtime process instance.
- `OrderProcess`: immutable snapshot of the template used by an order.
- `OrderSubProcess`: runtime state of one subprocess.
- `SubProcessDependency`: dependency edge between subprocesses.

## Audit and side effects

- `OrderEvent`: immutable event log entry.
- `OrderOutbox`: side effect delivery record.
