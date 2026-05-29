# 核心概念

## 模板层

- `OrderTemplate`：流程蓝图。
- `OrderTemplateStep`：子流程定义与状态规则。
- `FlowPath`：可选路径变体。

## 实例层

- `Order`：运行时流程实例。
- `OrderProcess`：订单使用的模板快照。
- `OrderSubProcess`：单个子流程的运行时状态。
- `SubProcessDependency`：子流程之间的依赖边。

## 审计与副作用

- `OrderEvent`：不可变事件日志。
- `OrderOutbox`：副作用投递记录。
