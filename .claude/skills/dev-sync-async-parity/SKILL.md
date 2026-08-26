---
name: dev-sync-async-parity
description: Sync/async parity rules for rhosocial-stateflow contributors - symmetric APIs, non-interoperable implementations, correct async model pairing, and the prohibition of asyncio.to_thread bridging
license: MIT
compatibility: opencode
metadata:
  category: architecture
  level: intermediate
  audience: developers
  order: 2
  prerequisites:
    - dev-backend-development
---

# Sync/Async Parity: Symmetric but Non-Interoperable

> **同步异步对等，但不互通。**

This skill covers the strict sync/async parity requirements for
`rhosocial-stateflow`. The framework provides **two completely parallel,
independent implementations** from the model layer upward. They share the
same API surface (method names, signatures) but **cannot be mixed** at any
layer.

## Core Principle: Two Closed Paths

```
Sync  path:  SyncModel  →  SyncBackend  →  SyncService  →  SyncDeliverer
Async path:  AsyncModel →  AsyncBackend →  AsyncService →  AsyncDeliverer
                                                     (full await chain)
```

Each path is self-contained. No layer may cross over.

### Why Non-Interoperable?

1. **Sync models** (`ActiveRecord` subclass) have **synchronous** `save()`,
   `query().all()`, `backend.transaction()` — calling these inside an
   `async def` **blocks the event loop**.
2. **Async models** (`AsyncActiveRecord` subclass) have **coroutine** `save()`,
   `query().all()`, `backend.transaction()` — they require a real
   `AsyncStorageBackend` and `await` at every call site.
3. **`asyncio.to_thread`** bridges sync code into async by running it in a
   thread pool. This is **prohibited** in stateflow because it:
   - Does not leverage async I/O (the whole point of async);
   - Introduces thread-safety issues (e.g., SQLite connections are
     bound to their creating thread);
   - Violates parity — the async path is not genuinely async.

## Model Layer: Sibling Classes

Every stateflow model has a **sync** and an **async** sibling, mapping to the
same database table but inheriting from different base classes:

```python
# Sync model
class Order(UUIDMixin, TimestampMixin, ActiveRecord):
    __table_name__ = "stateflow_orders"
    ...

# Async sibling — same table, same fields, async base
class AsyncOrder(UUIDMixin, TimestampMixin, AsyncActiveRecord):
    __table_name__ = "stateflow_orders"
    ...
```

Both classes share the same `__table_name__`, field declarations, and
business-logic helpers (`mark_completed`, `can_rollback`, etc.). They differ
only in their base class and the resulting query/save semantics.

### Naming Convention

| Sync | Async |
|------|-------|
| `Order` | `AsyncOrder` |
| `OrderSubProcess` | `AsyncOrderSubProcess` |
| `OrderEvent` | `AsyncOrderEvent` |
| `SyncOrderService` | `AsyncOrderService` |
| `SyncOrderDispatcher` | `AsyncOrderDispatcher` |
| `SyncOrderOutboxDeliverer` | `AsyncOrderOutboxDeliverer` |
| `SyncTimeoutScheduler` | `AsyncTimeoutScheduler` |
| `SyncHandlerStartTopic` | `AsyncHandlerStartTopic` |
| `SyncSubProcessHandler` | `AsyncSubProcessHandler` |

## Correct Usage

### ✅ Sync Path: Fully Synchronous

```python
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.stateflow import SyncOrderService, Order

config = SQLiteConnectionConfig(database=":memory:")
Order.configure(config, SQLiteBackend)

service = SyncOrderService()
result = service.publish_event(order_id=oid, subprocess_id=sid, new_status="locked")
```

Every call is synchronous. No `await`, no `asyncio.to_thread`.

### ✅ Async Path: Fully Async with `await`

```python
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.stateflow import AsyncOrderService, AsyncOrder

config = SQLiteConnectionConfig(database=":memory:")
AsyncOrder.configure(config, AsyncSQLiteBackend)

service = AsyncOrderService()
result = await service.publish_event(order_id=oid, subprocess_id=sid, new_status="locked")
```

Every DB operation (`save`, `query().all()`, `transaction()`) is a coroutine
natively provided by `AsyncActiveRecord` + `AsyncSQLiteBackend`.

## Prohibited Patterns

### ❌ `asyncio.to_thread` Bridging

```python
# WRONG: wrapping sync service in a thread to fake async
class AsyncOrderService:
    def __init__(self):
        self._sync = SyncOrderService()

    async def publish_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.publish_event, *args, **kwargs)
```

**Why it's wrong:** The sync service uses sync models (`Order.query().one()`
blocks). Wrapping it in `to_thread` runs that blocking code in a thread pool,
which:
- Does not perform actual async I/O;
- Breaks if the sync backend connection is not `check_same_thread=False`;
- Hides the fact that the "async" path isn't really async.

**Fix:** Define `AsyncOrder` (extending `AsyncActiveRecord`), use
`AsyncSQLiteBackend`, and write the service with real `await` calls:

```python
class AsyncOrderService:
    async def publish_event(self, order_id, subprocess_id, *, new_status, ...):
        async with AsyncOrder.backend().transaction():
            order = await AsyncOrder.query().where(AsyncOrder.c.id == order_id).one()
            subprocess = await AsyncOrderSubProcess.query().where(...).one()
            ...
            await subprocess.save()
            await result.event.save()
```

### ❌ Mixing Sync Models in Async Code

```python
# WRONG: async service references sync model
class AsyncOrderService:
    async def publish_event(self, ...):
        order = Order.query().where(...).one()  # sync model! blocks event loop
        order.save()  # sync save! blocks event loop
```

**Fix:** Use `AsyncOrder`, `AsyncOrderSubProcess`, etc. throughout.

### ❌ Async Model with Sync Backend

```python
# WRONG: async model configured with sync backend
AsyncOrder.configure(config, SQLiteBackend)  # should be AsyncSQLiteBackend
```

**Fix:** `AsyncOrder.configure(config, AsyncSQLiteBackend)`.

## Audit Checklist

When writing or reviewing async code, verify each item:

| Check | Requirement |
|-------|-------------|
| Model base class | `AsyncActiveRecord`, not `ActiveRecord` |
| Backend class | `AsyncSQLiteBackend` (or other `Async*Backend`) |
| `save()` / `delete()` | `await model.save()` |
| `query().all()` / `.one()` | `await Model.query().where(...).all()` |
| Transaction | `async with backend.transaction():` |
| Raw SQL | `await backend.execute(sql, params)` |
| No `asyncio.to_thread` | Must not appear in the async path |
| No sync model imports | Async path must not import sync model classes |

## Quick Reference

### Import Paths

```python
# Sync
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.stateflow import Order, SyncOrderService

# Async
from rhosocial.activerecord.model import AsyncActiveRecord
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.stateflow import AsyncOrder, AsyncOrderService
```

### Backend Availability

| Backend | Sync | Async |
|---------|------|-------|
| SQLite | `SQLiteBackend` | `AsyncSQLiteBackend` |
| MySQL | `MySQLBackend` | (check driver support) |
| PostgreSQL | `PostgreSQLBackend` | (check driver support) |

> If an async backend is not yet available for a database, the async path
> cannot be used for that database. Do **not** fall back to
> `asyncio.to_thread` — implement the async backend first.
