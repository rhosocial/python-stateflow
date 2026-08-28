# 快速开始

## 安装

```bash
pip install rhosocial-stateflow
```

开发安装：

```bash
pip install -e '.[test]'
```

## 同步路径

```python
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    SyncOrderFactory, SyncOrderService,
    Schema,
    Order, OrderTemplate, OrderTemplateStep,
)

# 1. 配置所有模型到同一个 backend（共享连接 + 事务）
config = SQLiteConnectionConfig(database=":memory:")
models = [Order, OrderTemplate, OrderTemplateStep]  # + 其他 6 个模型
with BackendGroup(name="stateflow", models=models,
                  config=config, backend_class=SQLiteBackend) as group:
    backend = group.get_backend()
    backend.connect()
    backend.introspect_and_adapt()
    Schema.create_tables(backend)

    # 2. 定义模板与步骤
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
    # ... payment, shipment 同理

    # 3. 工厂创建运行时实例
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

    # 4. 服务层推进状态（单事务：加载 → 调度 → 落库）
    service = SyncOrderService()
    result = service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",   # 幂等键
    )
    # result.started_subprocesses → ["payment"]（下游自动启动）
    # result.outbox_items → [handler_start outbox for payment]
```

## 异步路径

```python
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.activerecord.connection import AsyncBackendGroup
from rhosocial.stateflow import (
    AsyncOrderFactory, AsyncOrderService,
    Schema,
    AsyncOrder, AsyncOrderTemplate, AsyncOrderTemplateStep,
)

config = SQLiteConnectionConfig(database=":memory:")
async with AsyncBackendGroup(name="stateflow", models=[...],
                             config=config,
                             backend_class=AsyncSQLiteBackend) as group:
    backend = group.get_backend()
    await backend.connect()
    await backend.introspect_and_adapt()
    await Schema.async_create_tables(backend)

    # 异步工厂创建异步模型实例
    instance = await AsyncOrderFactory().create(template, steps, context={...})
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()

    # 异步服务层：全程 await，不阻塞事件循环
    service = AsyncOrderService()
    result = await service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",
    )
```

> **重要**：同步和异步路径不可混用。同步模型 (`Order`) 的 `save()` 是阻塞调用；
> 异步模型 (`AsyncOrder`) 的 `save()` 是协程。在异步代码中引用同步模型会阻塞事件循环。

## 状态分类规则

每个 `OrderTemplateStep` 声明三类终态：

| 字段 | 含义 | 约束 |
|------|------|------|
| `terminal_states` | 子流程的终态集合 | 必须声明所有可能的终态 |
| `advance_states` | 表示流程推进的终态 | 必须是 `terminal_states` 的子集 |
| `rollback_states` | 可触发回滚的终态 | 必须是 `terminal_states` 的子集 |
| `timeout_status` | 超时后转入的状态 | 必须在 `terminal_states` 中 |

校验由 `OrderTemplateValidator` 在工厂创建实例时执行，违反约束抛 `TemplateValidationError`。
