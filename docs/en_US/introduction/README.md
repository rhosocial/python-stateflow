# Introduction

`rhosocial-stateflow` is a general-purpose state transition framework.

It combines:

- finite state machines for subprocess state management;
- event-driven DAG orchestration for dependency-based progress;
- immutable event logs for auditing;
- outbox-based side effect delivery for reliable runtime integration.

The initial implementation is an MVP and focuses on the core model, template validation, factory, dispatcher, handler interfaces, and tests.
