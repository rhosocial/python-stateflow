# Rule: Sync/Async Parity is Non-Interoperable

> **同步异步对等，但不互通。**

## 核心原则

stateflow 的同步 (Sync) 与异步 (Async) API 是**完全对等的两套独立实现**，从模型层
（`ActiveRecord` / `AsyncActiveRecord`）到服务层（`SyncOrderService` /
`AsyncOrderService`）到投递器（`SyncOrderOutboxDeliverer` /
`AsyncOrderOutboxDeliverer`）全程平行。它们**不可混用**。

```
Sync  路径:  SyncModel  →  SyncBackend  →  SyncService  →  SyncDeliverer
Async 路径:  AsyncModel →  AsyncBackend →  AsyncService →  AsyncDeliverer
                                                        （全程 await）
```

两条路径各自封闭，任何一层都不得跨越。

## 正确用法

### ✅ 同步路径：全程同步

```python
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.stateflow import SyncOrderService, Order, OrderSubProcess

Order.configure(config, SQLiteBackend)
service = SyncOrderService()
result = service.publish_event(order_id=oid, subprocess_id=sid, new_status="locked")
```

### ✅ 异步路径：全程异步 + await

```python
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.stateflow import AsyncOrderService, AsyncOrder, AsyncOrderSubProcess

AsyncOrder.configure(config, AsyncSQLiteBackend)
service = AsyncOrderService()
result = await service.publish_event(order_id=oid, subprocess_id=sid, new_status="locked")
```

异步路径的每一层 DB 操作（`save()` / `query().all()` / `transaction()`）都是真正的
协程，由 `AsyncActiveRecord` + `AsyncStorageBackend` 提供原生支持。

## 错误用法（禁止）

### ❌ 用 asyncio.to_thread 桥接同步代码

```python
# 错误：把同步 service 包进线程冒充异步
async def publish_event(self, ...):
    return await asyncio.to_thread(self._sync_service.publish_event, ...)
```

这会：
1. 在事件循环中阻塞线程池，而非利用异步 I/O；
2. 引发线程安全问题（SQLite 连接绑定创建线程）；
3. 破坏同步/异步对等性——异步路径并未真正使用 async 模型。

### ❌ 混用同步模型与异步服务

```python
# 错误：异步服务内部使用同步模型
class AsyncOrderService:
    async def publish_event(self, ...):
        order = Order.query().where(...).one()  # 同步模型！阻塞事件循环
        order.save()  # 同步 save！阻塞事件循环
```

同步模型的 `query()` / `save()` 不是协程，直接调用会阻塞整个事件循环。

### ❌ 在异步路径中调用同步 backend

```python
# 错误：异步模型配置了同步 backend
AsyncOrder.configure(config, SQLiteBackend)  # 应该用 AsyncSQLiteBackend
```

## 判定清单

编写或审查异步代码时，逐项确认：

| 检查项 | 要求 |
|--------|------|
| 模型基类 | `AsyncActiveRecord`，而非 `ActiveRecord` |
| Backend 类 | `AsyncSQLiteBackend` 等异步 backend |
| `save()` / `delete()` | `await model.save()` |
| `query().all()` / `.one()` | `await Model.query().where(...).all()` |
| 事务 | `async with backend.transaction():` |
| 原生 SQL | `await backend.execute(sql, params)` |
| 无 `asyncio.to_thread` | 异步路径中不得出现 |
| 无同步模型引用 | 异步路径中不得 import 同步模型类 |
