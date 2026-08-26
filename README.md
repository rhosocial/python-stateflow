# rhosocial-stateflow

`rhosocial-stateflow` 是基于 `rhosocial-activerecord` 的通用状态流转与事件驱动 DAG 编排框架。

它以订单流程作为直觉隐喻，但核心能力面向任意有限状态机与有向无环流程：审批流、工单、任务编排、Agent 执行计划等。

## 包信息

- 仓库目录：`python-stateflow`
- 发行包名：`rhosocial-stateflow`
- import 包名：`rhosocial.stateflow`

## 架构概览

```
模板层          OrderTemplate / OrderTemplateStep / FlowPath
                          │ (factory)
实例层          Order / OrderProcess / OrderSubProcess / SubProcessDependency
                          │ (dispatcher)
审计与副作用      OrderEvent / OrderOutbox
```

调度器（`SyncOrderDispatcher` / `AsyncOrderDispatcher`）无状态：每次状态转换表示为一条不可变的 `OrderEvent`。Handler 调用、通知投递、定时器注册等副作用不直接执行，而是写入 `OrderOutbox` 后由独立投递器处理。

### 同步/异步对等但不互通

每一层都有完全对等的同步与异步两套实现，**不可混用**：

| 层 | 同步 | 异步 |
|----|------|------|
| 模型 | `Order`（`ActiveRecord`） | `AsyncOrder`（`AsyncActiveRecord`） |
| 服务 | `SyncOrderService` | `AsyncOrderService` |
| 投递器 | `SyncOrderOutboxDeliverer` | `AsyncOrderOutboxDeliverer` |
| 调度器 | `SyncOrderDispatcher` | `AsyncOrderDispatcher` |
| 超时扫描 | `SyncTimeoutScheduler` | `AsyncTimeoutScheduler` |
| Topic | `SyncHandlerStartTopic` | `AsyncHandlerStartTopic` |
| 工厂 | `SyncOrderFactory` | `AsyncOrderFactory` |

异步路径全程使用 `AsyncActiveRecord` + `AsyncStorageBackend` + 原生 `await`，不使用 `asyncio.to_thread` 桥接。详见 `.claude/rules/sync-async-non-interoperability.md`。

## 功能列表

- **模板 DAG 校验**：`OrderTemplateValidator` 检查状态集、依赖、环检测、FlowPath 引用
- **订单实例工厂**：`SyncOrderFactory` / `AsyncOrderFactory` 从模板快照生成运行时对象图
- **事务化服务层**：`SyncOrderService` / `AsyncOrderService` 在单事务内完成加载 → 推进 → 落库
  - `publish_event`：状态转换 + 幂等（`event_key`）+ 下游自动启动
  - `publish_timeout`：超时状态转换
  - `publish_rollback`：可逆子流程的回滚生命周期
  - 乐观并发冲突转为 `ConcurrentStateTransitionError`
- **Outbox 投递器**：`SyncOrderOutboxDeliverer` / `AsyncOrderOutboxDeliverer`
  - 独立投递循环 + 指数退避重试
  - `UnrecoverableDeliveryError` 标记不可恢复
  - `recover_stuck` 恢复卡在 `processing` 的条目
- **Handler 注册与动态加载**：`HandlerRegistry` + 可选 `importlib` 动态导入
  - `SyncHandlerStartTopic` / `AsyncHandlerStartTopic`：`handler_start` topic 标准实现
  - `SyncHandlerRollbackTopic` / `AsyncHandlerRollbackTopic`：`handler_rollback` topic 标准实现
- **回滚链路**：`can_rollback` → `begin_rollback` → handler.rollback() → `complete_rollback` / `fail_rollback`
- **超时调度**：`mark_running` 自动计算 `timeout_at`；`SyncTimeoutScheduler` / `AsyncTimeoutScheduler` 扫描到期子流程
- **多后端测试套件**：`tests/providers/` provider registry，已实现 SQLite sync/async provider
- **预置生产级组件**（`rhosocial.stateflow.applications`）：
  - `ApprovalFlow` — 内容审批（提交 → 审核 → 发布 / 驳回）
  - `TicketSystem` — 工单系统（创建 → 分配 → 并行开发+QA → 关闭）
  - `TaskOrchestration` — 任务编排（钻石 DAG + 超时/回滚）
  - `AgentPlan` — Agent 执行计划（步骤编排 + 失败补偿）
  - `MediaGenerationFlow` — 文生图/视频（积分冻结 → 生成 → 交付 / 退款）
  - `SeatBookingFlow` — 固定座位订票（选座 → 校验 → 支付 → 出票）
  - `external_services` — 支付/积分协议 + Mock 实现（同步/异步对等）

## 安装

```bash
pip install rhosocial-stateflow
```

开发安装：

```bash
pip install -e '.[test]'
```

## 快速开始

### 同步路径

```python
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    SyncOrderFactory, SyncOrderService, SyncTimeoutScheduler,
    create_tables, Order, OrderTemplate, OrderTemplateStep,
)

# 1. 配置模型
config = SQLiteConnectionConfig(database=":memory:")
with BackendGroup(name="stateflow", models=[Order, OrderTemplate, OrderTemplateStep],
                  config=config, backend_class=SQLiteBackend) as group:
    backend = group.get_backend()
    backend.connect()
    backend.introspect_and_adapt()
    create_tables(backend)

    # 2. 定义模板
    template = OrderTemplate(name="purchase", version=1)
    template.save()

    # 3. 创建订单实例
    instance = SyncOrderFactory().create(template, steps, context={"user_id": "u123"})
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()

    # 4. 推进状态
    service = SyncOrderService()
    service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",
    )
```

### 异步路径

```python
from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend
from rhosocial.activerecord.connection import AsyncBackendGroup
from rhosocial.stateflow import (
    AsyncOrderFactory, AsyncOrderService, AsyncTimeoutScheduler,
    async_create_tables, AsyncOrder, AsyncOrderTemplate, AsyncOrderTemplateStep,
)

config = SQLiteConnectionConfig(database=":memory:")
async with AsyncBackendGroup(name="stateflow", models=[AsyncOrder, AsyncOrderTemplate, AsyncOrderTemplateStep],
                             config=config, backend_class=AsyncSQLiteBackend) as group:
    backend = group.get_backend()
    await backend.connect()
    await backend.introspect_and_adapt()
    await async_create_tables(backend)

    instance = await AsyncOrderFactory().create(template, steps, context={"user_id": "u123"})
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()

    service = AsyncOrderService()
    await service.publish_event(
        order_id=instance.order.id,
        subprocess_id=instance.get_subprocess("inventory").id,
        new_status="locked",
        event_key="inventory-1",
    )
```

## 测试

```bash
pytest
```

测试覆盖：模板校验、工厂依赖传播、调度器幂等/下游启动/冲突/并发、Outbox 投递重试/恢复、Handler 注册/动态加载、回滚生命周期、超时调度、同步/异步全链路对等验证。

## 许可证

Apache License 2.0
