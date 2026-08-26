# Getting Started

## Installation

```bash
pip install rhosocial-stateflow
```

Development install:

```bash
pip install -e '.[test]'
```

## Sync Path

```python
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    SyncOrderFactory, SyncOrderService,
    create_tables,
    Order, OrderTemplate, OrderTemplateStep,
)

# 1. Configure all models on a single shared backend
config = SQLiteConnectionConfig(database=":memory:")
models = [Order, OrderTemplate, OrderTemplateStep]  # + 6 other models
with BackendGroup(name="stateflow", models=models,
                  config=config, backend_class=SQLiteBackend) as group:
    backend = group.get_backend()
    backend.connect()
    backend.introspect_and_adapt()
    create_tables(backend)

    # 2. Define template and steps
    template = OrderTemplate(name="purchase", version=1)
    template.save()
    inventory = OrderTemplateStep(
        template_id=template.id, name="inventory",
        handler_class="app.handlers.InventoryHandler",
        terminal_states=["locked", "failed"],
        advance_states=["locked"],
        rollback_states=["failed"],
        step_order=1,
    )
    inventory.save()
    # ... payment, shipment similarly

    # 3. Factory creates runtime instance
    instance = SyncOrderFactory().create(
        template, [inventory, payment, shipment],
        context={"user_id": "u123"},
    )
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for dep in instance.dependencies:
        dep.save()
    for event in instance.events:
        event.save()

    # 4. Service advances state (single transaction: load → dispatch → persist)
    service = SyncOrderService()
    result = service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",   # idempotency key
    )
    # result.started_subprocesses → ["payment"] (downstream auto-started)
    # result.outbox_items → [handler_start outbox for payment]
```

## Async Path

```python
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.activerecord.connection import AsyncBackendGroup
from rhosocial.stateflow import (
    AsyncOrderFactory, AsyncOrderService,
    async_create_tables,
    AsyncOrder, AsyncOrderTemplate, AsyncOrderTemplateStep,
)

config = SQLiteConnectionConfig(database=":memory:")
async with AsyncBackendGroup(name="stateflow", models=[...],
                             config=config,
                             backend_class=AsyncSQLiteBackend) as group:
    backend = group.get_backend()
    await backend.connect()
    await backend.introspect_and_adapt()
    await async_create_tables(backend)

    # Async factory creates async model instances
    instance = await AsyncOrderFactory().create(template, steps, context={...})
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()

    # Async service: native await throughout, no event-loop blocking
    service = AsyncOrderService()
    result = await service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",
    )
```

> **Important**: Sync and async paths are non-interoperable. Sync models (`Order`) have
> blocking `save()`; async models (`AsyncOrder`) have coroutine `save()`. Referencing sync
> models in async code blocks the event loop.

## State Classification Rules

Each `OrderTemplateStep` declares three categories of terminal states:

| Field | Meaning | Constraint |
|-------|---------|------------|
| `terminal_states` | All possible terminal states | Must declare every terminal state |
| `advance_states` | States that advance the process | Must be a subset of `terminal_states` |
| `rollback_states` | States that can trigger rollback | Must be a subset of `terminal_states` |
| `timeout_status` | State to transition to on timeout | Must be in `terminal_states` |

Validation is performed by `OrderTemplateValidator` at factory creation time; violations raise `TemplateValidationError`.
