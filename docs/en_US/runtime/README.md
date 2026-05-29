# Runtime

The runtime is centered on `SyncOrderDispatcher` and `AsyncOrderDispatcher`.

The dispatcher is designed to be stateless: current state is read from persisted models, and every transition is represented as an `OrderEvent`.

Side effects such as handler invocation, notification delivery, and timer registration should be represented by `OrderOutbox` records instead of being executed directly inside state transition logic.
