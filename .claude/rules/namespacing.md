# Rule: Internal References Are Fully Namespaced

> **内部引用完整命名空间化，避免命名冲突。**

## 目标

stateflow 的所有内部引用都必须可唯一溯源到定义处，避免与用户业务值
（如自定义 `terminal_state`）或其他框架的标签发生命名冲突。两条路径：

1. **import 导入**：内部代码必须从**定义模块**导入，禁止从聚合 `__init__` 再导出导入。
2. **标签区分**：框架保留的字符串标签（状态值 / 事件类型 / topic / source）
   必须带 `stateflow:` 命名空间前缀，禁止裸字符串。

## import 导入规则

### ❌ 错误：从聚合器导入

```python
from .models import Order          # models/__init__ 是聚合器
from rhosocial.stateflow import Schema, SyncOrderFactory  # 包级 __init__ 是聚合器
from rhosocial.stateflow.applications import ApprovalFlow  # 应用模块内部
```

### ✅ 正确：从定义模块导入

```python
from rhosocial.stateflow.models.order import Order
from rhosocial.stateflow.models.order_event import OrderEvent
from rhosocial.stateflow.schema import Schema
from rhosocial.stateflow.factory import SyncOrderFactory
from rhosocial.stateflow.types import EVENT_SP_TIMEOUT
```

聚合器（`rhosocial.stateflow` / `rhosocial.stateflow.models` / 各模型子包
`__init__`）**只服务公共 API**，内部代码不得依赖它。

## 标签命名空间规则

框架保留的标签统一使用 `stateflow:` 前缀，按分类二次命名：

| 类别 | 前缀 | 示例 |
|------|------|------|
| 事件类型 | `stateflow:event:` | `stateflow:event:sp_status_changed` |
| Outbox topic | `stateflow:topic:` | `stateflow:topic:handler_start` |
| Order 状态 | `stateflow:order:` | `stateflow:order:completed` |
| Template 状态 | `stateflow:template:` | `stateflow:template:draft` |
| Subprocess 保留状态 | `stateflow:subprocess:` | `stateflow:subprocess:running` |
| Subprocess source | `stateflow:source:` | `stateflow:source:dynamic` |
| Rollback 状态 | `stateflow:rollback:` | `stateflow:rollback:running` |
| Outbox 状态 | `stateflow:outbox:` | `stateflow:outbox:pending` |

**用户业务状态不受此约束**：`terminal_states` / `advance_states` /
`rollback_states` / `timeout_status` 以及 `HandlerResult.status` 是调用方
声明的业务值，**不得**添加 `stateflow:` 前缀。

## 未来 API 的红线：`emit()` 约定

任何将来引入的、以"标签"作为**第一个参数**的 API（如事件发射
`emit(tag, ...)`），**第一个参数必须是完整命名空间的标签**：

- 可以是 `stateflow:event:` 前缀的字符串常量（`types.py` 中定义）；
- 禁止传入裸字符串（如 `"sp_status_changed"`）；
- 禁止散落硬编码——所有标签常量集中定义在 `rhosocial.stateflow.types`。

这保证同一事件日志 / topic 命名空间中，stateflow 的标签与其他来源永不相撞。

## 判定清单

| 检查项 | 要求 |
|--------|------|
| 内部 import | 从定义模块导入，不经过聚合器 `__init__` |
| 框架标签 | 带 `stateflow:` 前缀，在 `types.py` 集中定义 |
| 业务状态 | 不加前缀（用户声明） |
| `emit()` 类 API | 首参必须是命名空间标签，禁止裸字符串 |
