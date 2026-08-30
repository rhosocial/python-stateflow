import uuid
from datetime import datetime, timezone

import pytest
from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.stateflow import Order, OrderEvent, OrderOutbox
from rhosocial.stateflow.models import (
    AsyncFlowPath,
    AsyncFlowPathQuery,
    AsyncOrder,
    AsyncOrderEvent,
    AsyncOrderEventQuery,
    AsyncOrderOutbox,
    AsyncOrderOutboxQuery,
    AsyncOrderProcess,
    AsyncOrderProcessQuery,
    AsyncOrderQuery,
    AsyncOrderSubProcess,
    AsyncOrderSubProcessQuery,
    AsyncOrderTemplate,
    AsyncOrderTemplateQuery,
    AsyncOrderTemplateStep,
    AsyncOrderTemplateStepQuery,
    AsyncSubProcessDependency,
    AsyncSubProcessDependencyQuery,
    FlowPath,
    FlowPathQuery,
    OrderEventQuery,
    OrderOutboxQuery,
    OrderProcess,
    OrderProcessQuery,
    OrderQuery,
    OrderSubProcess,
    OrderSubProcessQuery,
    OrderTemplate,
    OrderTemplateQuery,
    OrderTemplateStep,
    OrderTemplateStepQuery,
    SubProcessDependency,
    SubProcessDependencyQuery,
)
from rhosocial.stateflow.types import (
    ORDER_STATUS_RUNNING,
    OUTBOX_STATUS_PENDING,
    SUBPROCESS_STATUS_PENDING,
)


@pytest.fixture(autouse=True)
def configure_models_for_query_building():
    models = (
        FlowPath,
        OrderTemplate,
        OrderTemplateStep,
        Order,
        OrderProcess,
        OrderSubProcess,
        SubProcessDependency,
        OrderEvent,
        OrderOutbox,
    )
    config = SQLiteConnectionConfig(database=":memory:")
    for model in models:
        model.configure(config, SQLiteBackend)
    yield
    for model in models:
        model.backend().disconnect()


def test_query_helpers_export_model_references():
    assert FlowPathQuery.model is FlowPath
    assert OrderTemplateQuery.model is OrderTemplate
    assert OrderTemplateStepQuery.model is OrderTemplateStep
    assert OrderQuery.model is Order
    assert OrderProcessQuery.model is OrderProcess
    assert OrderSubProcessQuery.model is OrderSubProcess
    assert SubProcessDependencyQuery.model is SubProcessDependency
    assert OrderEventQuery.model is OrderEvent
    assert OrderOutboxQuery.model is OrderOutbox


def test_order_query_helpers_build_query():
    query = OrderQuery.by_status("stateflow:order:running")

    assert query.model_class is Order


def test_order_template_step_query_helpers_build_query():
    template_id = uuid.uuid4()
    query = OrderTemplateStepQuery.ordered(template_id)

    assert query.model_class is OrderTemplateStep


def test_order_subprocess_query_helpers_build_query():
    process_id = uuid.uuid4()
    query = OrderSubProcessQuery.pending(process_id)

    assert query.model_class is OrderSubProcess
    assert SUBPROCESS_STATUS_PENDING == "stateflow:subprocess:pending"


def test_order_event_query_helpers_build_query():
    query = OrderEventQuery.by_event_key("event-key")

    assert query.model_class is OrderEvent


def test_order_outbox_query_helpers_build_query():
    query = OrderOutboxQuery.pending()

    assert query.model_class is OrderOutbox
    assert OUTBOX_STATUS_PENDING == "stateflow:outbox:pending"


def test_more_query_helpers_build_query():
    template_id = uuid.uuid4()
    process_id = uuid.uuid4()
    subprocess_id = uuid.uuid4()
    depends_on_id = uuid.uuid4()
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    assert FlowPathQuery.with_start_from(template_id).model_class is FlowPath
    assert OrderTemplateQuery.by_name_version("signup", 1).model_class is OrderTemplate
    assert OrderQuery.completed_after(now).model_class is Order
    assert OrderProcessQuery.by_order_id(uuid.uuid4()).model_class is OrderProcess
    assert OrderSubProcessQuery.timeouts_due(now).model_class is OrderSubProcess
    assert SubProcessDependencyQuery.between(subprocess_id, depends_on_id).model_class is SubProcessDependency
    assert OrderEventQuery.conflicts().model_class is OrderEvent
    assert OrderOutboxQuery.by_event_id(event_id).model_class is OrderOutbox
    assert process_id


async def test_async_query_helpers_export_async_model_references():
    """Async query siblings exist and reference the async models (parity)."""
    assert AsyncFlowPathQuery.model is AsyncFlowPath
    assert AsyncOrderTemplateQuery.model is AsyncOrderTemplate
    assert AsyncOrderTemplateStepQuery.model is AsyncOrderTemplateStep
    assert AsyncOrderQuery.model is AsyncOrder
    assert AsyncOrderProcessQuery.model is AsyncOrderProcess
    assert AsyncOrderSubProcessQuery.model is AsyncOrderSubProcess
    assert AsyncSubProcessDependencyQuery.model is AsyncSubProcessDependency
    assert AsyncOrderEventQuery.model is AsyncOrderEvent
    assert AsyncOrderOutboxQuery.model is AsyncOrderOutbox


async def test_async_query_helpers_build_query():
    """Async query builders produce queries against the async models."""
    from rhosocial.activerecord.backend.impl.sqlite import AsyncSQLiteBackend

    async_models = (
        AsyncFlowPath,
        AsyncOrderTemplate,
        AsyncOrderTemplateStep,
        AsyncOrder,
        AsyncOrderProcess,
        AsyncOrderSubProcess,
        AsyncSubProcessDependency,
        AsyncOrderEvent,
        AsyncOrderOutbox,
    )
    for model in async_models:
        await model.configure(
            SQLiteConnectionConfig(database=":memory:"), AsyncSQLiteBackend
        )
    try:
        template_id = uuid.uuid4()
        process_id = uuid.uuid4()
        subprocess_id = uuid.uuid4()
        depends_on_id = uuid.uuid4()
        now = datetime.now(timezone.utc)

        assert AsyncFlowPathQuery.with_start_from(template_id).model_class is AsyncFlowPath
        assert AsyncOrderTemplateQuery.by_name_version("signup", 1).model_class is AsyncOrderTemplate
        assert AsyncOrderQuery.by_status(ORDER_STATUS_RUNNING).model_class is AsyncOrder
        assert AsyncOrderProcessQuery.by_order_id(uuid.uuid4()).model_class is AsyncOrderProcess
        assert AsyncOrderSubProcessQuery.timeouts_due(now).model_class is AsyncOrderSubProcess
        assert AsyncSubProcessDependencyQuery.between(
            subprocess_id, depends_on_id
        ).model_class is AsyncSubProcessDependency
        assert AsyncOrderEventQuery.by_event_key("event-key").model_class is AsyncOrderEvent
        assert AsyncOrderOutboxQuery.by_event_id(uuid.uuid4()).model_class is AsyncOrderOutbox
        assert process_id
    finally:
        for model in async_models:
            await model.backend().disconnect()
