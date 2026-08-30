# python-stateflow 重构开发计划（四大原则 + AI Agent 演示）

> **日期**: 2026-08-29
> **影响范围**: `src/rhosocial/stateflow/`（core + models + applications）、`tests/`、
>   `.claude/rules/`、上游 `python-activerecord`（仅依赖协调，不在此仓库改）
> **状态**: 已实施（Phase A/B/C/E 完成；Phase D 已核查、迁移被上游阻断）
> **基线**: `main@95f087b`（refactor: enforce member signature parity and eliminate module-level functions）

> **实施记录（2026-08-29）**：Phase A（标签命名空间化 + import 完整路径 + `namespacing.md` 规则）、
> Phase B（9 组 async Query 兄弟 + parity 测试扩展）、Phase C（AST 守护测试）、
> Phase E（`append_subprocess` + `subprocess_appended` + `AiAgentAssistant`）均已实现。
> Phase D 经 `generate_ddl()` 实测确认 D1/D2/D4 后，判定迁移被上游阻断，`schema.py` 维持手写 DDL。

> **跨后端修复记录（2026-08-30）**：七端缺陷已并行修复并验证——
> - core：D1（version 列）/D2（NOT NULL）/D3b（None→中性回退）已修复；
> - mysql/mariadb/firebird：UUID 写路径建议 `(UUID, bytes)→(UUID, str)`，mysql/mariadb/firebird
>   各自 73/73 全绿；
> - sqlserver：`IF OBJECT_ID(...) IS NULL` 建表守卫，73/73 全绿；
> - oracle：Vector 适配器 `list→str` 移除 + `user_tables` 计数守卫（PL/SQL 匿名块），feature 36/36
>   + applications 28/28 全绿；
> - firebird：`UnsupportedFeatureError` 改为 `EXECUTE BLOCK` + `SELECT COUNT(*) INTO` 守卫
>   （PSQL 的 IF 不支持 EXISTS、DECLARE 须位于 AS 与 BEGIN 之间、RDB$RELATION_NAME 需 TRIM），
>   stateflow 侧 `Schema.drop_tables` 按方言加同构 DROP 守卫、时间戳列 oracle/firebird 用真
>   TIMESTAMP（TEXT 列会令 firebird 驱动把 datetime 参数字符串化），**全量 73/73 全绿**；
> - 最终回归：sqlite/mysql/mariadb/postgres/firebird 全量 EXIT=0（oracle/sqlserver 服务器暂停，
>   恢复后补跑全量）。详见各后端仓库 `.claude/plan/2026-08-30/`。
>
> **跨后端验证记录（2026-08-29，修复前基线）**：
> - **SQLite / PostgreSQL**：73/73 全部通过（含 AI Agent 动态图、async Query、OOP 守护）。
> - **MySQL / MariaDB**：存在**基线既有**缺陷 —— uuid 适配器将 UUID 以二进制字节写入 utf8mb4 TEXT 列，
>   报 `Incorrect string value ... for column id/event_id`（errno 1366）。已在原始提交 `95f087b`
>   上用 `git stash` 复现，与本重构无关。两个后端同属 MySQL 家族、同根因。
> - **SQL Server（2025, 127.0.0.1:11435）**：provider 已新增并注册（`sqlserver_sync/async.py`）；
>   冒烟全流程可用，但 `Schema.create_tables` 在重复运行时报 `There is already an object named ...`
>   —— 因 **T-SQL 不支持 `CREATE TABLE IF NOT EXISTS`**，且连接库为 `master` 会残留对象。
>   按用户指示，mariadb/sqlserver 仍在完善中，缺陷仅记录、暂不修复。
> - **Oracle（23c Free, 127.0.0.1:11523）**：provider 已新增并注册（`oracle_sync/async.py`）；
>   存在**适配器冲突缺陷**（见下 D-O1），stateflow 全量测试大面积 ERROR。

---

## 0. 摘要

本次重构围绕四条原则展开，外加一个 AI Agent 演示应用：

| # | 原则 | 现状结论 |
|---|------|----------|
| 1 | 全面同步/异步对等 | **部分满足**，存在缺口：Query 辅助类无 async 兄弟；parity 测试覆盖不全 |
| 2 | 全面面向对象（无独立函数） | **已满足**，但缺自动化约束（无 lint/test 守护） |
| 3 | 内部引用完整命名空间化 | **未满足**：内部 import 不一致；字符串标签（event_type / topic / status）均未命名空间化 |
| 4 | 由 ActiveRecord 推导 DDL | **未就绪**：上游在 `feature/active-record-ddl` 分支，未合入未发布；且存在缺陷需要先上报 |
| + | AI Agent 演示 | 新增 `applications/ai_agent.py`，利用 **动态运行图**（`append_subprocess` + `sp_appended`） |

---

## 1. 原则一：同步/异步对等 —— 现状核查

### 1.1 已满足的部分

| 层 | 同步 | 异步 | 状态 |
|----|------|------|------|
| 模型（9 个 × 2） | `Order`… | `AsyncOrder`… | ✅ 全部齐备 |
| 工厂 | `SyncOrderFactory` | `AsyncOrderFactory` | ✅ |
| 调度器 | `SyncOrderDispatcher` | `AsyncOrderDispatcher` | ✅ |
| 服务 | `SyncOrderService` | `AsyncOrderService` | ✅ |
| 投递器 | `SyncOrderOutboxDeliverer` | `AsyncOrderOutboxDeliverer` | ✅ |
| 定时器 | `SyncTimeoutScheduler` | `AsyncTimeoutScheduler` | ✅ |
| Handler ABC | `SyncSubProcessHandler` | `AsyncSubProcessHandler` | ✅ |
| Topic | `SyncHandler{Start,Rollback}Topic` | `AsyncHandler{Start,Rollback}Topic` | ✅ |
| Schema | `create_tables` | `async_create_tables` | ✅ |

`tests/.../feature/test_sync_async_parity.py` 已覆盖上述 8 对类的方法名 / 签名结构 / 协程性。

### 1.2 缺口（本次需补齐）

- **Gap-1（核心缺口）**：`models/*/query.py` 的 Query 辅助类**只有同步兄弟**，没有 async 兄弟：
  - `OrderQuery` / `OrderSubProcessQuery` / `OrderEventQuery` / `OrderOutboxQuery` /
    `OrderProcessQuery` / `OrderTemplateQuery` / `OrderTemplateStepQuery` /
    `FlowPathQuery` / `SubProcessDependencyQuery`
  - 且这些类**硬编码** `model = <同步模型>`，无法复用给 async 路径。
  - 违反对等原则：`models/__init__.py` 只导出同步 Query 类。
  - 说明：当前服务层内部直接用 `Model.query()` 内联查询，Query 类尚未被内部使用，主要是公共 API 面，因此补齐成本低。
- **Gap-2**：parity 测试 `PAIRS` 只覆盖 8 对类，**未覆盖**：
  - Query 类（新增 async 兄弟后应加入）；
  - 模型业务方法对（`Order.mark_completed`、`OrderSubProcess.apply_status` 等 9 组模型兄弟的方法签名/协程性）；
  - `Schema` 的 `create_tables` / `async_create_tables` 对。
- **Gap-3**：异步 Query 助手补齐后，`AsyncOrderService` 中的内联查询可改为使用 `AsyncOrderQuery` 等（可选，保持一致性）。

> 结论：对等原则**方向正确、主链路齐备**，但 Query 层是真实的缺口，需要补全并纳入 parity 测试。

---

## 2. 原则二：全面面向对象（无独立函数）—— 现状核查

### 2.1 现状

- `src/` 下**不存在模块级函数**（grep `^def ` / `^async def ` 仅命中 `tests/`）。所有函数都是类内方法 / `@staticmethod` / `@classmethod`。✅
- `.claude/rules/sync-async-non-interoperability.md` 已明文规定「禁止模块级函数」，当前代码遵守。✅
- 注意点：
  - `validators.py::_validate_acyclic` 内含一个嵌套闭包 `visit()`（局部函数，非模块级）。按"所有函数都应放在类内"的严格字面可视为合规，但建议评审是否提取为 `@staticmethod`。
  - `types.py` 的模块级**常量**（`ORDER_STATUS_*`、`EVENT_*`、`OUTBOX_*`）与 `deliverer.py` 的模块级**类型别名**（`TopicHandler`）不属于"函数"，合规。

### 2.2 缺口

- **缺自动化约束**：目前靠人工审查。应增加一条 AST 级 pytest（或 ruff 规则），扫描 `src/rhosocial/stateflow/**/*.py`，断言不存在模块级 `def` / `async def` / `lambda` 赋值。

> 结论：原则二**已满足**，仅需补充自动化守护，防止未来回退。

---

## 3. 原则三：完整命名空间化 —— 现状核查与方案

### 3.1 现状核查

**（a）内部 import 不一致**：
- 部分文件经包级再导出导入：`dispatcher.py` / `factory.py` / `service.py` 等用 `from .models import Order`、`from .types import ...`；
- 部分文件直接导入子模块：`order_subprocess/model.py` 用 `from ..order_process import OrderProcess`、`from ..order_template_step import OrderTemplateStep`；
- 路径引用风格不统一，命名冲突风险（尤其是未来接入更多应用时）。

**（b）字符串标签未命名空间化**（这是本次重点）：
- 事件类型：`EVENT_SP_STATUS_CHANGED = "sp_status_changed"`、`"order_created"` 等 —— 裸字符串；
- Outbox topic：`"handler_start"` / `"handler_rollback"` / `"notification"` / `"timer"` —— 裸字符串；
- 状态值：`"pending"` / `"running"` / `"completed"` / `"rolled_back"`、rollback 状态、subprocess `source` —— 裸字符串；
- 用户自定义的 `terminal_states` 等业务状态**不属于框架保留字**，无需强制命名空间化，但框架保留字必须隔离。

### 3.2 方案（需在评审时拍板两点）

1. **标签表示形式**（二选一，倾向 A）：
   - **A. 命名空间化 Enum**（推荐，对齐上游 `rhosocial.activerecord.interface.ModelEvent` 的 Enum 约定）：
     `StateflowEventType` / `StateflowTopic` / `OrderStatus` / `SubProcessStatus` / `RollbackStatus` / `SubProcessSource`，
     成员名带前缀，如 `StateflowEventType.SP_STATUS_CHANGED`；**持久化值**统一为 `"stateflow:event:sp_status_changed"` 这类带命名空间前缀的字符串（Enum 的 `value`）。
   - B. 纯字符串常量带前缀：`"stateflow:event:..."`、`"stateflow:topic:..."`（改动小，但无类型约束）。
   - 两者都需评估**对已持久化数据的破坏**（项目处于 1.0.0.dev1 开发期，可接受破坏性变更，但要写进 changelog）。
2. **`emit()` 约定**（用户指定示例）：未来任何 `emit(tag, ...)` 类 API，**第一个参数必须是完整命名空间的标签**（Enum 成员或带前缀的字符串常量），禁止裸字符串；并在 `types.py` 集中定义，禁止散落硬编码。当前代码无 `emit()`，此约定作为**新代码红线**写入规则文档，AI Agent 演示代码即为首个遵守者。

**（c）内部 import 统一**：
- 内部模块统一改为**完整路径导入**（如 `from rhosocial.stateflow.models.order import Order`），减少对包级 `__init__` 再导出的隐式依赖，避免将来命名冲突与循环导入。
- `__init__.py` 的再导出**只服务公共 API**，内部代码不得依赖它。

---

## 4. 原则四：由 ActiveRecord 推导 DDL —— 现状核查 + 上游缺陷清单

### 4.1 现状

- stateflow 的 `schema.py` 手写 9 张表的 `CreateTableExpression`（~280 行 `_col()` 样板），这正是上游 DDL 计划点名的"断层典型缩影"。
- 上游 `python-activerecord` 的 `feature/active-record-ddl` 分支已完成：
  - Phase 1：`UseSqlType` / `UseIndex` / `UseConstraint` / `TableOptions` / `DDLMixin`（`src/.../base/{ddl_mixin,ddl_handlers,fields}.py`）；
  - Phase 2：`ModelSchemaGenerator` + `ActiveRecord.generate_ddl()`（`src/.../base/ddl_generator.py`）；
  - `ActiveRecord` / `AsyncActiveRecord` 已内置 `DDLMixin` + `ColumnNameMixin`，stateflow 模型**无需改基类即可调用 `generate_ddl()`**。
- **未合入 main、未发布**：stateflow `pyproject.toml` 依赖 `rhosocial-activerecord>=1.0.0.dev0,<2.0.0`（解析到发布版，无 `generate_ddl`）。必须先协调上游合并 + 发版 + 本仓库升级依赖。

### 4.2 上游缺失 / 缺陷清单（先提出，供上游修复）

按影响 stateflow 的程度排序：

| # | 缺陷 / 缺失 | 位置 | 影响 |
|---|-------------|------|------|
| D1 | **`_version`（OptimisticLockMixin 的 PrivateAttr）不在 `model_fields` 中**，`generate_ddl()` 不会生成 `version` 列 | `field/version.py:105` + `base/ddl_generator.py` | stateflow `OrderSubProcess` 依赖 `version` 列做乐观锁；推导 DDL 将**丢列**。需上游提供"私有/隐藏字段也可声明为列"的机制（如 `__ddl_include_private_fields__` 或显式 `__constraints__`/`UseSqlType` 之外的路） |
| D2 | **必填字段不自动生成 NOT NULL**：`_build_columns` 只加 PK，不含 `not_null`，与 docstring 声称不符 | `base/ddl_generator.py:121-166` | stateflow 手写 DDL 大量 `not_null=True`；推导后必填列变为可空，语义回退 |
| D3 | **各后端 `suggest_column_type` 覆盖不全**：核心仓库仅 SQLite + Dummy 实现；各独立后端仓库覆盖不齐 | `backend/dialect/mixins/ddl_type.py` + 各后端仓库 | 若后端未实现，`_resolve_data_type` 回退到 `IntegerType()`（ddl_generator.py:192），**所有列静默变 INTEGER**，危险 |
| D4 | **datetime 类型变化**：stateflow 现手写 DDL 用 `TextType()` 存 datetime；推导后 `datetime → DateTimeType()` | `base/ddl_generator.py` 中性映射 | 列类型改变；需在全部目标后端验证 `DateTimeType` 编译 + 值往返（SQLite 尤其） |
| D5 | `dict`/`list` → `TextType()` 中性映射 | 同上 | 与 stateflow 现行为一致（JSON 存 TEXT）；增强项：Postgres 用 JSONB / MySQL 用 JSON（`UseSqlType` 可按方言覆盖） |
| D6 | 列顺序：`model_fields` 中 mixin 字段顺序与手写 DDL 顺序可能不一致（PK 是否排首、`version` 位置） | `base/ddl_generator.py:_build_columns` | 影响建表整洁与 DDL 对比；需验证 |
| D7 | 无"从模型收集 `ALL_MODELS`"的辅助；`Schema.ALL_MODELS` 现硬编码 | 本仓库 `schema.py` | 迁移后应改为由模型元数据推导（或保留显式元组供 drop 逆序） |
| D8 | `UseSqlType` 文档声称 `"pg"`/`"PostgreSQL"` 大小写不敏感，已实现；但中性映射无 `Decimal` 长度/精度建议 | `base/fields.py:149-161` | 低优先级，stateflow 暂用不到 |

**实测确认（2026-08-29，SQLite dialect，`Model.generate_ddl()` 对 9 个模型输出）**：

- ✅ **D1 确认**：`stateflow_order_subprocesses` 推导 DDL **缺少 `version` 列**（`OptimisticLockMixin._version` 为 PrivateAttr，不在 `model_fields`）。乐观锁将直接失效。
- ✅ **D2 确认**：推导 DDL **全部列无 `NOT NULL`**（含 `name`/`status`/`handler_class` 等必填列）。
- ⚠️ **D4 补充**：SQLite 下 `datetime`/`bool` 被映射为 `NUMERIC`（现手写 schema 用 `TEXT`/`BOOLEAN`），列类型语义变化，需验证值往返。
- ℹ️ 列顺序：mixin 字段（`created_at`/`updated_at`/`id`）排在业务字段之前，与手写 DDL 的 `id` 排首不同（仅观感差异）。

**跨方言 `suggest_column_type` 覆盖实测（2026-08-29，对 `Order` 模型 `generate_ddl()`）**：

| 后端 | `suggest_column_type` | 实测 DDL 关键类型 | 结论 |
|------|----------------------|-------------------|------|
| PostgreSQL | ✅ 已实现 | `uuid→UUID`、`dict→JSONB`、`datetime→TIMESTAMP` | 良好（仍缺 `version`/NOT NULL，D1/D2） |
| MySQL | ✅ 已实现 | `uuid→BINARY(16)`、**`dict→INT`** | **缺陷**：`dict/list→None` 后生成器回退 `IntegerType` |
| MariaDB | ✅ 已实现 | `uuid→BINARY(16)`、**`dict→INT`** | 同 MySQL 缺陷 |
| SQL Server | ❌ **未实现** | **所有列→INT** | **缺陷**：未实现时整表回退 `IntegerType`，完全不可用 |
| SQLite | ✅ 已实现 | `datetime/bool→NUMERIC` | D4 见上 |

**新增缺陷（D3 实证细化）**：
- **D3a**：`sqlserver` 方言完全未实现 `suggest_column_type` → `generate_ddl()` 所有列静默变 `INT`。
- **D3b**：`mysql`/`mariadb` 方言的 `suggest_column_type` 对 `dict`/`list` 返回 `None`，而
  `ModelSchemaGenerator._resolve_data_type` 在方言返回 `None` 时**直接回退 `IntegerType`**（未回退到中性
  `TextType` 建议）。stateflow 的 `context`/`payload`/`extra`/`template_snapshot` 等 JSON 列将变 `INT`。
  修复方向：方言返回 `None` 时应落到中性建议（`_NEUTRAL_TYPE_SUGGESTIONS`），而非 `IntegerType`。

**跨后端运行验证（2026-08-29，stateflow 全量测试）**：
- PostgreSQL：**73/73 通过**（含 AI Agent 动态图）。
- SQLite：73/73 通过。
- MySQL / MariaDB：基线既有 UUID 字符集缺陷（`Incorrect string value ... id/event_id`），见"实施记录"。
- SQL Server：T-SQL 不支持 `CREATE TABLE IF NOT EXISTS`（重复运行报对象已存在），provider 已就位待完善。
- Oracle：见下 D-O1 适配器冲突，feature 类测试大面积 ERROR。

**三后端项目专项探索（2026-08-29，实证）**：

| # | 项目 | 缺陷 | 位置 | 实证 |
|---|------|------|------|------|
| D-M1 | mariadb | **UUID 二进制存储**：`MariaDBUUIDAdapter.to_database` 在 `target_type is bytes` 时返回 `value.bytes`（16 字节），注册表同时登记 `str`/`bytes` 两个目标；任何模型（含 stateflow `CHAR(36)` 列）保存 UUID 时以二进制写入 → `Incorrect string value ... for column id` (1366) | `python-activerecord-mariadb/.../adapters.py:96` | `UUIDMixin` 模型探针直接复现 |
| D-S1 | sqlserver | **`CREATE TABLE IF NOT EXISTS` 不支持**：`supports_if_not_exists_table=False`，`format_create_table_statement` 直接**忽略** `if_not_exists` 标志（不翻译成存在性守卫），重复执行建表报 `There is already an object named ...` (2714) | `python-activerecord-sqlserver/.../dialect.py:974,1250-1271` | stateflow `Schema.create_tables` 二次运行复现 |
| D-O1 | oracle | **Vector 适配器冲突**：`OracleVectorAdapter` 注册 `list→str`（`adapters.py:323`），与 `OracleJSONAdapter` 的 `list→str`（`adapters.py:168`）冲突，且列适配器按类型名选、list 列被当作向量解析 → `could not convert string to float: '"locked"'`。stateflow 的 `terminal_states`/`payload`/`context` 等 list/dict 列全部受影响，feature 测试大面积 ERROR | `python-activerecord-oracle/.../adapters.py:315-337` | stateflow `publish_event` 直接复现 |

> 三个缺陷均为**后端项目**既有问题，与 stateflow 重构无关；已按用户指示记录，mariadb/sqlserver/oracle 仍在完善中。

**结论**：D1/D2 为阻断项。`schema.py` **暂不迁移**（当前手写 DDL 保持正确），待上游合并 + 发版 + 修复后再执行 4.3；迁移前需在模型上声明 `UseSqlType`/`UseConstraint`（NOT NULL、`version`）以对齐语义。

### 4.3 本仓库迁移方案（依赖上游 D1–D3 修复 + 发版后）

- 在 9 个模型上声明需要的 DDL 元标记（`UseSqlType` / `UseConstraint` / `__indexes__` 等），尽量保持列语义与现手写 DDL 一致（NOT NULL、类型、`version` 列）；
- 重写 `schema.py` 为 `Model.generate_ddl(dialect, if_not_exists=True).to_sql()` 循环执行，删除手写 `_col/_table/_table_expressions`；`drop_tables` 保留；
- 保留 `Schema.create_tables` / `async_create_tables` 公共签名（外部依赖不变）；
- 跨后端回归：SQLite/MySQL/MariaDB/PostgreSQL 4 端建表 + 全测试通过；
- 若 D1 短期无法修复，提供**临时兼容层**：`Schema` 对 `version` 列手动补齐（`_extra_column("version", ...)`），并加注释待上游修复后移除。

---

## 5. AI Agent 演示应用

### 5.1 目标

在 `applications/` 下新增一个 **动态运行图** 的 AI Agent 演示（区别于现有 `AgentPlan` 的固定线性图），充分体现 stateflow "运行时动态定义 DAG" 的能力。

### 5.2 设计

- **文件**：`src/rhosocial/stateflow/applications/ai_agent.py`（类名与导出遵循现有风格，如 `AiAgentAssistant`）。
- **种子模板**：仅一个 `plan` 步骤（`terminal_states=["planned","plan_failed"]`, `advance_states=["planned"]`）。
- **动态扩展**：`plan` 步骤的 handler 读取 `order.context["task"]`，在 `start()` 内调用 `SyncOrderFactory.append_subprocess`（async 用 `AsyncOrderFactory.append_subprocess`），按任务动态追加工具步骤链（如 `search → write → review`，或按需分支的小 DAG），每个动态步骤依赖前序步骤；
- **动态步骤**：每个步骤是工具调用 handler（`SearchToolHandler` / `WriteToolHandler` / `ReviewHandler` 等），带 `terminal/advance/rollback_states`，失败可触发补偿（rollback）；
- **编排器**：提供 `run()`（sync）+ `async run()` 便捷方法：创建实例 → 持久化 → 发布 `plan` 事件 → handler 动态追加 → 逐个推进到完成（或失败 + 回滚补偿路径）；
- **对等性**：`sync_registry()` / `async_registry()`、`build_template()` / `build_async_template()`、`models` / `async_models` 全部双套；
- **遵守新规**：命名空间化标签、无模块级函数、完整路径 import、`emit()` 约定。

### 5.3 需要的核心小增强（`sp_appended` 落地）

`EVENT_SP_APPENDED = "sp_appended"`（types.py:36）已定义但**无任何使用**：

- 为 `OrderEvent` / `AsyncOrderEvent` 增加 `subprocess_appended(order, subprocess, event_key=None)` 工厂方法；
- 为 `SyncOrderService` / `AsyncOrderService` 增加事务化 `append_subprocess(...)`：持久化新 subprocess + 依赖边 + `sp_appended` 事件，使**动态运行图成为一等公民能力**（与 `publish_event` 同构：load → append → persist）。

### 5.4 测试

- `tests/rhosocial/stateflow_test/applications/test_ai_agent.py`（+ async 版），沿用现有应用测试模式（`backend_group` / `async_backend_group` fixture）：
  1. happy path：`plan → 动态追加 search→write→review → completed`；
  2. 动态失败 + 补偿：某动态步骤 `*_failed` → `publish_rollback` 逆序补偿；
  3. 动态追加幂等/持久化：`sp_appended` 事件、依赖边正确、二次追加去重。

---

## 6. 任务分解（实施顺序）

> 每项任务遵循 conventional commits + towncrier changelog 片段（`changelog.d/`）。

### Phase A：命名空间化（原则三，先行——后续改动都依赖它）
- [ ] A1 评审拍板：Enum vs 前缀字符串、命名空间前缀字符串格式（建议 `stateflow:event:` / `stateflow:topic:`）
- [ ] A2 定义命名空间化 Enum/常量，替换 `types.py` 全部裸字符串，贯穿 models/dispatcher/service/deliverer/timer/registry/applications/tests
- [ ] A3 统一内部 import 为完整路径；收紧 `__init__.py` 为纯公共 API
- [ ] A4 更新 `.claude/rules/` + skill：`emit()` 首参命名空间约定、禁止裸标签
- [ ] A5 全量测试回归（4 端 provider）

### Phase B：同步/异步对等补齐（原则一）
- [ ] B1 为 9 个模型补齐 async Query 兄弟类（`AsyncOrderQuery` 等），参数化 `model`
- [ ] B2 扩展 `test_sync_async_parity.py`：Query 类、模型业务方法对、Schema 对 纳入校验
- [ ] B3 （可选）`AsyncOrderService` 改用 async Query 助手

### Phase C：面向对象自动化约束（原则二）
- [ ] C1 新增 AST 级 pytest（或 ruff 规则）：`src/rhosocial/stateflow/` 下禁止模块级 `def`/`async def`/`lambda` 赋值
- [ ] C2 评审 `validators._validate_acyclic` 嵌套 `visit()` 是否提取为 `@staticmethod`

### Phase D：DDL 推导迁移（原则四，依赖上游）
- [ ] D0 上报 D1–D3 缺陷至 python-activerecord；跟踪合并 + 发版
- [ ] D1 升级本仓库依赖并锁定含 `generate_ddl` 的版本
- [ ] D2 9 个模型声明 DDL 元标记（NOT NULL、类型、`version` 列等），必要时加临时兼容层
- [ ] D3 重写 `schema.py` 使用 `generate_ddl()`，删除手写样板
- [ ] D4 4 端 provider 建表 + 全测试回归；DDL 输出对比（推导 vs 原手写）

### Phase E：AI Agent 演示（原则一/三/四 的落地范例）
- [ ] E1 核心增强：`OrderEvent.subprocess_appended` + 服务层事务化 `append_subprocess`
- [ ] E2 `applications/ai_agent.py`（动态运行图 + 双套 + 命名空间化 + 无模块级函数）
- [ ] E3 `tests/.../applications/test_ai_agent.py`（sync + async）

### 收尾
- [ ] 全量测试（SQLite 必跑；MySQL/MariaDB/PostgreSQL 视服务器可用性）
- [ ] `ruff check src/` + `mypy src/`
- [ ] changelog 片段 + 文档（`docs/` README 增补 AI Agent 演示与命名空间约定）

---

## 7. 验证方式

- 单测：`PYTHONPATH=tests .venv…/bin/pytest tests/`（至少 SQLite 全绿；有服务器则 4 端）
- parity 测试：`test_sync_async_parity.py` 扩展后覆盖 Query 类与模型业务方法
- OOP 守护：新增 AST 测试直接纳入 `tests/`
- DDL：新增 `schema.py` 生成 DDL 与期望断言的单元测试 + 4 端建表冒烟
- 代码检查：`ruff check src/`、`mypy src/`

---

## 8. 风险与协调

| 风险 | 说明 | 缓解 |
|------|------|------|
| 上游 DDL 未及时合并/发版 | Phase D 被阻塞 | 先完成 Phase A/B/C/E；D 缺陷已提前上报 |
| 命名空间化破坏已存数据 | `event_type`/`topic`/`status` 值变更 | dev 阶段可接受；写 changelog；Enum 方案可加 value 别名迁移 |
| Query async 兄弟改动面 | 公共 API 变化 | 先补测试再加实现，保持签名结构一致 |
| 动态 append 与乐观锁/幂等 | 服务层新增事务方法需保证 `event_key` 幂等 | 复用 `publish_event` 的幂等模式 |
