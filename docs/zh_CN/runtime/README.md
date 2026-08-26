# 运行时

## 调度器 (Dispatcher)

`SyncOrderDispatcher` / `AsyncOrderDispatcher` 是**无状态**的纯逻辑组件：输入内存对象，输出状态变更 + 事件 + outbox 项，不执行任何 I/O。

### 核心方法

#### `on_event`

处理子流程状态转换。流程：

```mermaid
flowchart TD
    A["收到 new_status + event_key"] --> B{"event_key 已存在?"}
    B -->|是| C["返回 duplicate=True"]
    B -->|否| D{"子流程 skipped?"}
    D -->|是| E["抛 InvalidStateTransitionError"]
    D -->|否| F{"已终态?"}
    F -->|是| G{"new_status == 当前状态?"}
    G -->|是| H["创建 status_changed 事件"]
    G -->|否| I["创建 conflict 事件"]
    F -->|否| J["apply_status(new_status)<br/>创建 sp_status_changed 事件"]
    J --> K{"new_status 是 advance?"}
    K -->|是| L["_start_ready_subprocesses<br/>下游 mark_running + handler_start outbox"]
    K -->|否| M["无下游启动"]
    L --> N{"所有子流程 advance?"}
    N -->|是| O["order.mark_completed<br/>+ order_completed 事件"]
    N -->|否| P["返回 DispatchResult"]
    O --> P
    H --> P
    I --> P
    C --> P
    M --> N
```

#### `on_timeout`

包装 `on_event`，使用子流程的 `timeout_status` 作为目标状态。

#### `on_rollback`

开始可逆子流程的回滚：

1. **幂等检查**：同 `on_event`
2. **资格检查**：`can_rollback()` 不满足则抛 `InvalidStateTransitionError`
3. `begin_rollback()` → 创建 `sp_rollback_started` 事件 + `handler_rollback` outbox

实际 handler 调用由 outbox 投递器异步执行。

### DispatchResult

```python
class DispatchResult:
    event: OrderEvent              # 产生的事件
    started_subprocesses: list     # 启动的下游子流程
    outbox_items: list             # 待投递的 outbox 条目
    duplicate: bool               # 是否为重复事件
```

### Sync/Async 实现

调度逻辑通过 `_DispatcherBase` 共享，子类仅覆盖 `_event_cls` / `_outbox_cls`：

- `SyncOrderDispatcher` → 产生 `OrderEvent` / `OrderOutbox`
- `AsyncOrderDispatcher` → 产生 `AsyncOrderEvent` / `AsyncOrderOutbox`

`AsyncOrderDispatcher` 的 `async def` 方法存在以保持 API 对等。`on_timeout` 需重写以 `await self.on_event()`（基类的 `self.on_event()` 在异步路径返回协程）。

## 服务层 (Service)

`SyncOrderService` / `AsyncOrderService` 在单事务内完成**加载 → 调度 → 落库**：

### publish_event

```mermaid
sequenceDiagram
    participant C as 调用方
    participant S as SyncOrderService
    participant DB as Database
    participant D as Dispatcher

    C->>S: publish_event(order_id, subprocess_id, new_status, event_key)
    S->>DB: BEGIN TRANSACTION
    S->>DB: 加载 Order (by order_id)
    S->>DB: 加载 SubProcess → 获取 process_id
    S->>DB: 加载全部 SubProcesses (by process_id)
    S->>DB: 加载 Dependencies + 现有 Events
    S->>D: on_event(order, subprocess, ...)
    D-->>S: DispatchResult (event + started + outbox)

    alt duplicate (event_key 已存在)
        S-->>C: DispatchResult(duplicate=True)
    else 正常推进
        S->>DB: subprocess.save() (乐观锁检查)
        S->>DB: started_subprocesses.save()
        S->>DB: order.save() (若 mark_completed)
        S->>DB: event.save()
        S->>DB: outbox.save()
        S->>DB: COMMIT
        S-->>C: DispatchResult
    else 乐观锁失败
        DB-->>S: DatabaseError("updated by another process")
        S-->>C: ConcurrentStateTransitionError
    end
```

**并发处理**：`OrderSubProcess` 的 `OptimisticLockMixin` 在 UPDATE 时检查 `version`。
若版本不匹配（`affected_rows == 0`），`save()` 抛 `DatabaseError`，服务层转为
`ConcurrentStateTransitionError`。

**幂等**：`event_key` 在加载的现有事件中查重，匹配则 `duplicate=True`，不写任何数据。

### publish_timeout

同 `publish_event`，但调用 `dispatcher.on_timeout()`。

### publish_rollback

同结构，但调用 `dispatcher.on_rollback()`，产出 `sp_rollback_started` 事件 + `handler_rollback` outbox。

## Outbox 投递器 (Deliverer)

`SyncOrderOutboxDeliverer` / `AsyncOrderOutboxDeliverer` 独立扫描 pending outbox 条目并投递。

### 投递循环

```mermaid
flowchart TD
    A["deliver_pending(limit)"] --> B["claim_next_pending<br/>pending → processing"]
    B --> C{"找到条目?"}
    C -->|否| Z["返回 processed 计数"]
    C -->|是| D["调用 topic handler(item)"]
    D --> E{"结果?"}
    E -->|True| F["mark_sent"]
    E -->|False| G["mark_retryable<br/>retry_count++ + 指数退避"]
    E -->|UnrecoverableDeliveryError| H["mark_failed"]
    E -->|其他异常| G
    G --> I{"retry_count > max_retries?"}
    I -->|是| H
    I -->|否| J["status = pending + next_retry_at"]
    F --> K["processed++"]
    H --> K
    J --> K
    K --> L{"processed < limit?"}
    L -->|是| B
    L -->|否| Z
```

### Topic 注册

```python
deliverer.register_topic_handler("handler_start", topic_callable)
```

Topic handler 是返回 `bool` 的可调用对象（异步路径返回 `Awaitable[bool]`）。

### 恢复机制

`recover_stuck(stuck_after)`：将 `processing` 状态超过 `stuck_after` 时长的条目重置为 `pending`，
用于崩溃恢复。不增加 `retry_count`。

## Handler 注册 (Registry)

### HandlerRegistry

```python
registry = HandlerRegistry(allow_dynamic_import=False)
registry.register("app.handlers.PaymentHandler", PaymentHandler)
```

- 显式注册优先
- `allow_dynamic_import=True` 时，对 `"module.attr"` 格式的 key 尝试 `importlib.import_module`
- `instantiate(key, subprocess)` 返回 `SyncSubProcessHandler` 实例
- `instantiate_async(key, subprocess)` 返回 `AsyncSubProcessHandler` 实例

### 标准 Topic 实现

#### SyncHandlerStartTopic / AsyncHandlerStartTopic

`handler_start` topic 的标准实现：

1. 从 outbox payload 取 `subprocess_id` → 加载子流程 + order_id
2. `registry.instantiate(handler_class, subprocess)` → handler 实例
3. `handler.start()` → `HandlerResult`
4. 若 result 有 status → `service.publish_event()` 推进状态
5. 返回 `True`（投递器标记 sent）

未注册的 handler_class → `UnknownHandlerError` → `UnrecoverableDeliveryError`（不可恢复）

#### SyncHandlerRollbackTopic / AsyncHandlerRollbackTopic

`handler_rollback` topic 的标准实现：

1. 加载子流程 + handler
2. `handler.rollback()` → `HandlerResult`
3. 若 result 有 status → `service.publish_event()` 推进状态
4. `subprocess.complete_rollback()` + save
5. 异常 → `fail_rollback(error)` + 创建 `sp_rollback_failed` 事件

## 回滚生命周期

```mermaid
flowchart TD
    A["子流程到达 rollback_state<br/>(如 status='failed')"] --> B["publish_rollback"]
    B --> C["rollback_status = running<br/>+ sp_rollback_started 事件<br/>+ handler_rollback outbox"]
    C --> D["Outbox 投递器处理"]
    D --> E["handler.rollback()"]
    E --> F{"结果?"}
    F -->|成功 + 有 status| G["publish_event(result.status)<br/>推进子流程状态"]
    F -->|成功 + 无 status| H["无状态转换"]
    F -->|异常| I["fail_rollback(error)<br/>+ sp_rollback_failed 事件"]
    G --> J["complete_rollback()<br/>rollback_status = completed"]
    H --> J
    I --> K["rollback_status = failed<br/>rollback_error 记录"]
```

`can_rollback()` 条件：`is_reversible=True` + `rollback_status=not_required` + 当前状态在 `rollback_states` 中。

幂等：重复 `publish_rollback` 同一 `event_key` → `duplicate=True`。

## 超时调度 (Timer)

### timeout_at 计算

`OrderSubProcess.mark_running()` 在设置 `started_at` 的同时计算：
`timeout_at = started_at + timedelta(seconds=timeout_seconds)`（仅当 `timeout_seconds` 不为 None）。

### 扫描器

`SyncTimeoutScheduler` / `AsyncTimeoutScheduler`：

```python
scheduler = SyncTimeoutScheduler(service)
processed = scheduler.tick()  # 扫描到期子流程 → service.publish_timeout
```

- 查询：`skipped == False AND timeout_at <= now`
- 每个子流程独立事务处理，单个失败不影响其他
- `limit` 参数控制单次扫描上限
