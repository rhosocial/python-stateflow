# 运行时

运行时以 `SyncOrderDispatcher` 和 `AsyncOrderDispatcher` 为核心。

调度器设计为无状态：当前状态来自持久化模型，每次状态转换都表示为一条 `OrderEvent`。

Handler 调用、通知投递、定时器注册等副作用不应在状态转换逻辑中直接执行，而应写入 `OrderOutbox` 后由独立投递器处理。
