# tests/rhosocial/stateflow_test/feature/test_deliverer.py
"""Outbox deliverer tests: claim → invoke → outcome."""

from datetime import datetime, timedelta, timezone

import pytest

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.stateflow import (
    OrderOutbox,
    SyncOrderOutboxDeliverer,
    UnrecoverableDeliveryError,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.types import (
    OUTBOX_STATUS_FAILED,
    OUTBOX_STATUS_PENDING,
    OUTBOX_STATUS_PROCESSING,
    OUTBOX_STATUS_SENT,
    OUTBOX_TOPIC_HANDLER_START,
)



@pytest.fixture
def backend_group():
    """Single in-memory SQLite backend with the outbox table created."""
    from rhosocial.stateflow import (
        FlowPath,
        Order,
        OrderEvent,
        OrderProcess,
        OrderSubProcess,
        OrderTemplate,
        OrderTemplateStep,
        SubProcessDependency,
    )

    models = (
        OrderTemplate, OrderTemplateStep, FlowPath, Order, OrderProcess,
        OrderSubProcess, SubProcessDependency, OrderEvent, OrderOutbox,
    )
    config = SQLiteConnectionConfig(database=":memory:")
    with BackendGroup(
        name="deliverer-test", models=list(models), config=config, backend_class=SQLiteBackend,
    ) as group:
        backend = group.get_backend()
        backend.connect()
        backend.introspect_and_adapt()
        create_tables(backend)
        yield group
        drop_tables(backend)


def _seed_outbox(topic=OUTBOX_TOPIC_HANDLER_START, payload=None):
    """Insert one pending outbox item and return the persisted row."""
    item = OrderOutbox(event_id=__import__("uuid").uuid4(), topic=topic, payload=payload or {})
    item.save()
    return item


def test_deliver_pending_marks_sent_on_success(backend_group):
    """A returning-True handler flips the item to sent and clears next_retry_at."""
    _seed_outbox()
    deliverer = SyncOrderOutboxDeliverer()
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, lambda item: True)

    processed = deliverer.deliver_pending()

    assert processed == 1
    sent = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_SENT).all()
    assert len(sent) == 1
    assert sent[0].next_retry_at is None


def test_deliver_pending_retries_on_false_with_backoff(backend_group):
    """A returning-False handler schedules next_retry_at and keeps the item pending."""
    _seed_outbox()
    deliverer = SyncOrderOutboxDeliverer(max_retries=3, base_delay=2.0)
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, lambda item: False)

    first_pass = deliverer.deliver_pending()
    assert first_pass == 1

    pending = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_PENDING).all()
    assert len(pending) == 1
    assert pending[0].retry_count == 1
    assert pending[0].next_retry_at is not None

    # next_retry_at is in the future (exponential backoff: base_delay * 2^0 = 2s)
    now = datetime.now(timezone.utc)
    assert pending[0].next_retry_at > now


def test_deliver_pending_marks_failed_after_max_retries(backend_group):
    """Once retry_count exceeds max_retries the item is marked failed, not retried."""
    _seed_outbox()
    deliverer = SyncOrderOutboxDeliverer(max_retries=2, base_delay=0.0)
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, lambda item: False)

    deliverer.deliver_pending()  # retry_count -> 1
    deliverer.deliver_pending()  # retry_count -> 2
    deliverer.deliver_pending()  # retry_count -> 3 > max_retries -> failed

    failed = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_FAILED).all()
    assert len(failed) == 1
    assert failed[0].retry_count == 3
    assert failed[0].next_retry_at is None


def test_deliver_pending_marks_failed_on_unrecoverable_error(backend_group):
    """An UnrecoverableDeliveryError short-circuits retries and marks failed."""
    _seed_outbox()

    def always_unrecoverable(item):
        raise UnrecoverableDeliveryError("permanent failure")

    deliverer = SyncOrderOutboxDeliverer()
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, always_unrecoverable)

    deliverer.deliver_pending()

    failed = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_FAILED).all()
    assert len(failed) == 1


def test_deliver_pending_retries_on_generic_exception(backend_group):
    """A generic exception is treated as retryable, not unrecoverable."""
    _seed_outbox()

    def boom(item):
        raise RuntimeError("transient network error")

    deliverer = SyncOrderOutboxDeliverer(max_retries=1, base_delay=0.0)
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, boom)

    deliverer.deliver_pending()  # retry_count -> 1
    deliverer.deliver_pending()  # retry_count -> 2 > max_retries -> failed

    failed = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_FAILED).all()
    assert len(failed) == 1


def test_deliver_pending_unknown_topic_marks_failed(backend_group):
    """An outbox item with no registered handler is marked failed immediately."""
    _seed_outbox(topic="custom.topic")
    deliverer = SyncOrderOutboxDeliverer()

    deliverer.deliver_pending()

    failed = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_FAILED).all()
    assert len(failed) == 1


def test_deliver_pending_respects_limit(backend_group):
    """limit caps how many items one sweep processes."""
    for _ in range(3):
        _seed_outbox()
    deliverer = SyncOrderOutboxDeliverer()
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, lambda item: True)

    processed = deliverer.deliver_pending(limit=2)

    assert processed == 2
    remaining = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_PENDING).all()
    assert len(remaining) == 1


def test_recover_stuck_returns_processing_to_pending(backend_group):
    """Items left in processing longer than the cutoff are reset to pending."""
    item = _seed_outbox()
    backend = OrderOutbox.backend()
    # Bypass TimestampMixin (which would otherwise reset updated_at to now on
    # save) by writing the processing state and a stale updated_at via raw SQL.
    stale = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    backend.execute(
        "UPDATE stateflow_order_outbox SET status = ?, updated_at = ? WHERE id = ?",
        (OUTBOX_STATUS_PROCESSING, stale, str(item.id)),
    )

    deliverer = SyncOrderOutboxDeliverer()
    recovered = deliverer.recover_stuck(timedelta(minutes=5))

    assert recovered == 1
    reset = OrderOutbox.query().where(OrderOutbox.c.id == item.id).one()
    assert reset.status == OUTBOX_STATUS_PENDING


def test_deliver_skips_items_not_yet_due(backend_group):
    """An item with next_retry_at in the future is not picked up."""
    item = _seed_outbox()
    item.next_retry_at = datetime.now(timezone.utc) + timedelta(hours=1)
    item.save()

    deliverer = SyncOrderOutboxDeliverer()
    deliverer.register_topic_handler(OUTBOX_TOPIC_HANDLER_START, lambda item: True)

    processed = deliverer.deliver_pending()
    assert processed == 0
    pending = OrderOutbox.query().where(OrderOutbox.c.status == OUTBOX_STATUS_PENDING).all()
    assert len(pending) == 1

