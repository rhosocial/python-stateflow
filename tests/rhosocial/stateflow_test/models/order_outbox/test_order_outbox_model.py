import uuid

from rhosocial.stateflow import OrderOutbox
from rhosocial.stateflow.types import OUTBOX_STATUS_PENDING, OUTBOX_TOPIC_HANDLER_START


class Event:
    id = uuid.uuid4()


class SubProcess:
    id = uuid.uuid4()


def test_handler_start_builds_outbox_item():
    outbox = OrderOutbox.handler_start(Event(), SubProcess())

    assert outbox.event_id == Event.id
    assert outbox.topic == OUTBOX_TOPIC_HANDLER_START
    assert outbox.payload == {"subprocess_id": str(SubProcess.id)}
    assert outbox.status == OUTBOX_STATUS_PENDING
