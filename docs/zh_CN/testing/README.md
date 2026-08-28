# 测试

## 运行测试

```bash
cd python-stateflow
pytest
```

当前 120 个测试覆盖：

| 分类 | 测试数 | 覆盖内容 |
|------|--------|----------|
| 模板校验 | 6 | DAG 环检测、状态集约束、超时状态、FlowPath 引用 |
| 工厂 | 4 | 依赖传播、skip_steps、start_from、动态追加 |
| 调度器 | 4 | 幂等、下游启动、终态冲突、跳过拒绝 |
| 服务层 | 10 | 状态推进+持久化、下游启动+outbox、幂等、订单完成、冲突事件、跳过拒绝、并发错误、超时 |
| Outbox 投递器 | 10 | 成功/重试/失败/不可恢复/未知topic/limit/恢复/非到期跳过 |
| Handler 注册 | 8 | 显式注册/动态导入/unknown handler + handler_start topic 驱动 |
| 回滚链路 | 8 | 资格检查/启动/完成/失败记录/幂等/双重回滚拒绝 |
| 超时调度 | 5 | timeout_at 计算/到期触发/未到期跳过/limit |
| 异步路径 | 8 | AsyncSQLiteBackend 全链路：service/deliverer/timer/registry/rollback |
| 同步/异步对等 | 3 | 方法名对等 + 协程检查 |
| 模型 + Query | 39 | 9 个模型的 CRUD + Query helper 构建 |
| 应用组件 | 23 | 7 个预置组件（ApprovalFlow/TicketSystem/TaskOrchestration/AgentPlan/MediaGenerationFlow/SeatBookingFlow）同步+异步全链路 |

## 多后端 Provider 架构

```
tests/
├── conftest.py              # 从 provider 获取 backend fixture
└── providers/
    ├── __init__.py
    ├── base.py              # StateflowSyncProvider / StateflowAsyncProvider 协议
    ├── registry.py          # get_sync_provider() / get_async_provider()
    ├── sqlite_sync.py       # SQLiteSyncProvider（内存 SQLite + BackendGroup）
    └── sqlite_async.py      # SQLiteAsyncProvider（内存 SQLite + AsyncBackendGroup）
```

### 工作原理

1. `conftest.py` 通过 `sys.path.insert` 使 `providers` 可导入
2. `backend_group` fixture 调用 `sync_provider.setup()` → 配置模型 + 建表
3. 测试用 `backend_group` fixture 获取已配置的 backend
4. 测试结束后 `sync_provider.teardown()` 清理

### 添加新后端

以 MySQL 同步为例：

```python
# tests/providers/mysql_sync.py
from .base import StateflowSyncProvider

class MySQLSyncProvider(StateflowSyncProvider):
    @property
    def name(self):
        return "mysql-sync"

    @property
    def models(self):
        return (OrderTemplate, OrderTemplateStep, ...)  # 同步模型

    def setup(self):
        config = MySQLConnectionConfig(host="localhost", ...)
        group = BackendGroup(name="stateflow-mysql", models=list(self.models),
                             config=config, backend_class=MySQLBackend)
        group.configure()
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        Schema.create_tables(backend)  # 需要后端特定的 DDL
        return group

    def teardown(self, handle):
        drop_tables(handle.get_backend())
        handle.disconnect()
```

```python
# tests/providers/registry.py — 注册新 provider
from .mysql_sync import MySQLSyncProvider

def get_sync_providers():
    return [SQLiteSyncProvider(), MySQLSyncProvider()]
```

### 测试命令

```bash
# 只跑同步测试
pytest -k "not async"

# 只跑异步测试
pytest tests/rhosocial/stateflow_test/feature/test_async_path.py
```
