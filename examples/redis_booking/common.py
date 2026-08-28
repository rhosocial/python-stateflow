# examples/redis_booking/common.py
"""共享配置:redis 连接、队列键名、stateflow 后端 setup/teardown、订单处理。

本范例用 SQLite :memory: 作为 stateflow 后端(快、零配置),
redis 仅作请求登记队列(生产者-消费者缓冲),不存业务数据。
座位分配/余票扣减在 stateflow 处理侧,redis 不参与业务。
"""

import json
import logging
import time

import redis

logging.disable(logging.CRITICAL)

from rhosocial.activerecord.backend.impl.sqlite import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.connection import BackendGroup

from rhosocial.stateflow import (
    SyncOrderFactory,
    SyncOrderService,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.applications import SeatBookingFlow
from rhosocial.stateflow.applications.external_services import MockPaymentService

REDIS_HOST = "localhost"
REDIS_PORT = 6379

QUEUE_KEY = "stateflow:booking:requests"
RESULT_PREFIX = "stateflow:booking:result:"
STATUS_PREFIX = "stateflow:booking:status:"
RESULT_TTL = 3600

STOCK_TABLE = "demo_stock"
DEFAULT_EVENT = "concert"

PRICE = 8800


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def setup_stateflow(stock_event=None, stock_total=None):
    config = SQLiteConnectionConfig(database=":memory:")
    models = list(SeatBookingFlow.models)
    group = BackendGroup(
        name="redis-demo", models=models, config=config, backend_class=SQLiteBackend
    )
    group.configure()
    b = group.get_backend()
    b.connect()
    b.introspect_and_adapt()
    create_tables(b)
    if stock_event and stock_total is not None:
        b.execute(
            f"CREATE TABLE IF NOT EXISTS {STOCK_TABLE} "
            f"(event TEXT PRIMARY KEY, total INTEGER, remaining INTEGER)"
        )
        b.execute(
            f"INSERT OR IGNORE INTO {STOCK_TABLE} (event, total, remaining) "
            f"VALUES (?, ?, ?)",
            (stock_event, stock_total, stock_total),
        )
    template, steps = SeatBookingFlow.build_template()
    template.save()
    for s in steps:
        s.save()
    ctx = (template, steps, SyncOrderFactory(), SyncOrderService(), MockPaymentService())
    return group, ctx


def deduct_stock(backend, event=DEFAULT_EVENT):
    """原子扣减一张票。返回 True=抢到,False=无票(短路)。

    UPDATE ... WHERE remaining>0 是单语句原子操作,行锁保护,
    多 worker 并发也安全。affected_rows=0 即余票已尽。
    """
    result = backend.execute(
        f"UPDATE {STOCK_TABLE} SET remaining = remaining - 1 "
        f"WHERE event = ? AND remaining > 0",
        (event,),
    )
    return getattr(result, "affected_rows", 0) > 0


def teardown_stateflow(group):
    b = group.get_backend()
    try:
        drop_tables(b)
    except Exception:
        pass
    try:
        group.disconnect()
    except Exception:
        pass


def _persist(inst):
    inst.order.save()
    inst.process.save()
    for sp in inst.subprocesses:
        sp.save()
    for d in inst.dependencies:
        d.save()
    for e in inst.events:
        e.save()


def process_request(ctx, payload):
    template, steps, factory, service, payment = ctx
    rid = payload["request_id"]
    seat = payload.get("seat_pref") or f"A-{rid[-4:]}"
    inst = factory.create(
        template, steps,
        context={"seat_id": seat, "price": PRICE, "event": payload.get("event", "concert")},
    )
    for sp in inst.subprocesses:
        sp.extra = {"seat_id": seat, "price": PRICE}
        sp.save()
    _persist(inst)
    oid = inst.order.id
    psp = inst.get_subprocess("payment")
    tx = payment.charge(oid, PRICE)
    psp.extra["tx_id"] = tx
    psp.save()
    for label, spn, stt in [
        ("sel", "select_seat", "seat_selected"),
        ("val", "validate", "validated"),
        ("pay", None, "paid"),
        ("tkt", "issue_ticket", "ticketed"),
    ]:
        sid = psp.id if spn is None else inst.get_subprocess(spn).id
        service.publish_event(
            order_id=oid, subprocess_id=sid, new_status=stt, event_key=f"{label}-{rid}"
        )
    return {"request_id": rid, "order_id": str(oid), "status": "completed"}


def make_payload(seq, user_id=None, seat_pref=None):
    return {
        "request_id": f"REQ-{seq}",
        "user_id": user_id or f"u{seq}",
        "seq": seq,
        "event": "concert",
        "seat_pref": seat_pref or f"A-{seq}",
        "ts": time.time(),
    }
