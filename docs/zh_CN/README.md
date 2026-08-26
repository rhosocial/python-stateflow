# rhosocial-stateflow

`rhosocial-stateflow` 是构建在 `rhosocial-activerecord` 之上的状态流转与事件驱动 DAG 编排框架。

虽然核心模型沿用了订单相关命名，但框架目标是通用流程：审批流、工单、任务编排、Agent 执行计划等都可以用同一套抽象表达。

## 目录

- [简介](introduction/README.md) — 设计目标、核心能力、同步/异步对等原则、模块结构
- [快速开始](getting_started/README.md) — 安装、同步/异步路径示例、状态分类规则
- [核心概念](concepts/README.md) — 三层架构、9 个模型详解、同步/异步模型兄弟
- [运行时](runtime/README.md) — 调度器、服务层、Outbox 投递器、Handler 注册、回滚生命周期、超时调度
- [测试](testing/README.md) — 测试覆盖（120 个测试）、多后端 Provider 架构、添加新后端
