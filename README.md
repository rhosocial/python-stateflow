# rhosocial-stateflow

`rhosocial-stateflow` 是基于 `rhosocial-activerecord` 的通用状态流转与事件驱动 DAG 编排框架。

它以订单流程作为直觉隐喻，但核心能力面向任意有限状态机与有向无环流程：审批流、工单、任务编排、Agent 执行计划等。

## 包信息

- 仓库目录：`python-stateflow`
- 发行包名：`rhosocial-stateflow`
- import 包名：`rhosocial.stateflow`

## 当前状态

当前版本是 MVP 原型，包含：

- 核心 ActiveRecord 数据模型
- 模板 DAG 校验
- 订单实例工厂
- 同步/异步接口对等骨架
- 基础事件调度器
- Outbox 数据结构

## 安装

```bash
pip install rhosocial-stateflow
```

开发安装：

```bash
pip install -e '.[test]'
```
