# rhosocial-stateflow

`rhosocial-stateflow` is a state transition and event-driven DAG orchestration framework built on `rhosocial-activerecord`.

While the core models use order-related naming, the framework targets general-purpose workflows: approval flows, ticket systems, task orchestration, agent execution plans, and more.

## Table of Contents

- [Introduction](introduction/README.md) — Design goals, core capabilities, sync/async parity principle, module structure
- [Getting Started](getting_started/README.md) — Installation, sync/async quickstart with code, state classification rules
- [Core Concepts](concepts/README.md) — Three-layer architecture, 9 models explained, sync/async model siblings
- [Runtime](runtime/README.md) — Dispatcher, service layer, outbox deliverer, handler registry, rollback lifecycle, timeout scheduling
- [Testing](testing/README.md) — Test coverage (120 tests), multi-backend provider architecture, adding new backends
