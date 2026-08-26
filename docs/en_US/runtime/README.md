# Runtime

## Dispatcher

`SyncOrderDispatcher` / `AsyncOrderDispatcher` are **stateless** pure-logic components: they take in-memory objects, produce state changes + events + outbox items, and perform no I/O.

### Core Methods

#### `on_event`

Handles a subprocess status transition:

```mermaid
flowchart TD
    A["Receive new_status + event_key"] --> B{"event_key exists?"}
    B -->|Yes| C["Return duplicate=True"]
    B -->|No| D{"Subprocess skipped?"}
    D -->|Yes| E["Raise InvalidStateTransitionError"]
    D -->|No| F{"Already terminal?"}
    F -->|Yes| G{"new_status == current?"}
    G -->|Yes| H["Create status_changed event"]
    G -->|No| I["Create conflict event"]
    F -->|No| J["apply_status(new_status)<br/>Create sp_status_changed event"]
    J --> K{"new_status is advance?"}
    K -->|Yes| L["_start_ready_subprocesses<br/>downstream mark_running + handler_start outbox"]
    K -->|No| M["No downstream to start"]
    L --> N{"All subprocesses advance?"}
    N -->|Yes| O["order.mark_completed<br/>+ order_completed event"]
    N -->|No| P["Return DispatchResult"]
    O --> P
    H --> P
    I --> P
    C --> P
    M --> N
```

#### `on_timeout`

Wraps `on_event` using the subprocess's `timeout_status` as the target state.

#### `on_rollback`

Begins a reversible subprocess's rollback:

1. **Idempotency check**: same as `on_event`
2. **Eligibility check**: `can_rollback()` fails → `InvalidStateTransitionError`
3. `begin_rollback()` → create `sp_rollback_started` event + `handler_rollback` outbox

The actual handler call is performed asynchronously by the outbox deliverer.

### DispatchResult

```python
class DispatchResult:
    event: OrderEvent              # produced event
    started_subprocesses: list     # downstream subprocesses started
    outbox_items: list             # outbox items to deliver
    duplicate: bool                # whether this was a duplicate event
```

### Sync/Async Implementation

Dispatch logic is shared via `_DispatcherBase`; subclasses only override `_event_cls` / `_outbox_cls`:

- `SyncOrderDispatcher` → produces `OrderEvent` / `OrderOutbox`
- `AsyncOrderDispatcher` → produces `AsyncOrderEvent` / `AsyncOrderOutbox`

The `async def` methods on `AsyncOrderDispatcher` exist for API parity. `on_timeout` is overridden to `await self.on_event()` (the base `self.on_event()` returns a coroutine in the async path).

## Service Layer

`SyncOrderService` / `AsyncOrderService` perform **load → dispatch → persist** in a single transaction:

### publish_event

```mermaid
sequenceDiagram
    participant C as Caller
    participant S as SyncOrderService
    participant DB as Database
    participant D as Dispatcher

    C->>S: publish_event(order_id, subprocess_id, new_status, event_key)
    S->>DB: BEGIN TRANSACTION
    S->>DB: Load Order (by order_id)
    S->>DB: Load SubProcess → get process_id
    S->>DB: Load all SubProcesses (by process_id)
    S->>DB: Load Dependencies + existing Events
    S->>D: on_event(order, subprocess, ...)
    D-->>S: DispatchResult (event + started + outbox)

    alt duplicate (event_key exists)
        S-->>C: DispatchResult(duplicate=True)
    else normal advance
        S->>DB: subprocess.save() (optimistic lock check)
        S->>DB: started_subprocesses.save()
        S->>DB: order.save() (if mark_completed)
        S->>DB: event.save()
        S->>DB: outbox.save()
        S->>DB: COMMIT
        S-->>C: DispatchResult
    else optimistic lock failure
        DB-->>S: DatabaseError("updated by another process")
        S-->>C: ConcurrentStateTransitionError
    end
```

**Concurrency**: `OrderSubProcess` carries `OptimisticLockMixin`; UPDATE checks `version`.
If the version no longer matches (`affected_rows == 0`), `save()` raises `DatabaseError`,
which the service converts to `ConcurrentStateTransitionError`.

**Idempotency**: `event_key` is checked against loaded existing events; a match returns `duplicate=True` without writing anything.

### publish_timeout

Same structure as `publish_event`, but calls `dispatcher.on_timeout()`.

### publish_rollback

Same structure, but calls `dispatcher.on_rollback()`, producing a `sp_rollback_started` event + `handler_rollback` outbox.

## Outbox Deliverer

`SyncOrderOutboxDeliverer` / `AsyncOrderOutboxDeliverer` independently scan pending outbox items and deliver them.

### Delivery Loop

```mermaid
flowchart TD
    A["deliver_pending(limit)"] --> B["claim_next_pending<br/>pending → processing"]
    B --> C{"Item found?"}
    C -->|No| Z["Return processed count"]
    C -->|Yes| D["Call topic handler(item)"]
    D --> E{"Result?"}
    E -->|True| F["mark_sent"]
    E -->|False| G["mark_retryable<br/>retry_count++ + backoff"]
    E -->|UnrecoverableDeliveryError| H["mark_failed"]
    E -->|Other exception| G
    G --> I{"retry_count > max_retries?"}
    I -->|Yes| H
    I -->|No| J["status = pending + next_retry_at"]
    F --> K["processed++"]
    H --> K
    J --> K
    K --> L{"processed < limit?"}
    L -->|Yes| B
    L -->|No| Z
```

### Topic Registration

```python
deliverer.register_topic_handler("handler_start", topic_callable)
```

A topic handler is a callable returning `bool` (async path returns `Awaitable[bool]`).

### Recovery

`recover_stuck(stuck_after)`: resets `processing` items older than `stuck_after` back to `pending`,
for crash recovery. Does not increment `retry_count`.

## Handler Registry

### HandlerRegistry

```python
registry = HandlerRegistry(allow_dynamic_import=False)
registry.register("app.handlers.PaymentHandler", PaymentHandler)
```

- Explicit registration takes priority
- With `allow_dynamic_import=True`, `"module.attr"` keys are resolved via `importlib.import_module`
- `instantiate(key, subprocess)` returns a `SyncSubProcessHandler` instance
- `instantiate_async(key, subprocess)` returns an `AsyncSubProcessHandler` instance

### Standard Topic Implementations

#### SyncHandlerStartTopic / AsyncHandlerStartTopic

Standard implementation of the `handler_start` topic:

1. Extract `subprocess_id` from outbox payload → load subprocess + order_id
2. `registry.instantiate(handler_class, subprocess)` → handler instance
3. `handler.start()` → `HandlerResult`
4. If result has status → `service.publish_event()` to advance state
5. Return `True` (deliverer marks sent)

Unregistered handler_class → `UnknownHandlerError` → `UnrecoverableDeliveryError` (non-retryable)

#### SyncHandlerRollbackTopic / AsyncHandlerRollbackTopic

Standard implementation of the `handler_rollback` topic:

1. Load subprocess + handler
2. `handler.rollback()` → `HandlerResult`
3. If result has status → `service.publish_event()` to advance state
4. `subprocess.complete_rollback()` + save
5. On exception → `fail_rollback(error)` + create `sp_rollback_failed` event

## Rollback Lifecycle

```mermaid
flowchart TD
    A["Subprocess reaches rollback_state<br/>(e.g. status='failed')"] --> B["publish_rollback"]
    B --> C["rollback_status = running<br/>+ sp_rollback_started event<br/>+ handler_rollback outbox"]
    C --> D["Outbox deliverer processes"]
    D --> E["handler.rollback()"]
    E --> F{"Result?"}
    F -->|Success + has status| G["publish_event(result.status)<br/>advance subprocess state"]
    F -->|Success + no status| H["No state transition"]
    F -->|Exception| I["fail_rollback(error)<br/>+ sp_rollback_failed event"]
    G --> J["complete_rollback()<br/>rollback_status = completed"]
    H --> J
    I --> K["rollback_status = failed<br/>rollback_error recorded"]
```

`can_rollback()` requires: `is_reversible=True` + `rollback_status=not_required` + current status is in `rollback_states`.

Idempotent: repeating `publish_rollback` with the same `event_key` → `duplicate=True`.

## Timeout Scheduling

### timeout_at Computation

`OrderSubProcess.mark_running()` sets `started_at` and, when `timeout_seconds` is not None, computes:
`timeout_at = started_at + timedelta(seconds=timeout_seconds)`.

### Sweeper

`SyncTimeoutScheduler` / `AsyncTimeoutScheduler`:

```python
scheduler = SyncTimeoutScheduler(service)
processed = scheduler.tick()  # scan due subprocesses → service.publish_timeout
```

- Query: `skipped == False AND timeout_at <= now`
- Each subprocess handled in its own transaction; a single failure doesn't affect others
- `limit` parameter caps the number of items per sweep
