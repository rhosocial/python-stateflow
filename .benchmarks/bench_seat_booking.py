# .benchmarks/bench_seat_booking.py
"""基准测试:stateflow 直接接流(无前置缓冲)的最大压力。

支持后端:SQLite / MySQL。

三个维度:
1. 串行基线 —— 单订单完整 happy path 的耗时分解
   (factory.create / persist / 4x publish_event 各占多少),
   折算串行 TPS,即单连接吞吐上限。
2. async 并发 —— 不同并发度下的 TPS + P50/P95/P99 延迟。
   注:stateflow 单进程单 BackendGroup = 单连接(async_backend.py:161,
   async connector 不支持连接池)。单连接下多协程并发开事务可能冲突,
   本段用 wait_for 超时保护,如实报告各后端单连接的并发行为。
3. 乐观锁冲突风暴 —— 同一 order 同一 subprocess 被并发 publish,
   测乐观锁冲突率。单连接串行执行会规避冲突;真冲突需多进程。

边界:只测 stateflow 引擎开销(publish_event 内部 load->dispatch->persist),
不执行 outbox handler 投递(handler 是用户业务,不属引擎固有开销)。
"""

import argparse
import asyncio
import logging
import sys
import time

logging.disable(logging.CRITICAL)

from rhosocial.activerecord.backend.impl.sqlite import (
    AsyncSQLiteBackend,
    SQLiteBackend,
)
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig
from rhosocial.activerecord.backend.impl.mysql import (
    AsyncMySQLBackend,
    MySQLBackend,
)
from rhosocial.activerecord.backend.impl.mysql.config import MySQLConnectionConfig
from rhosocial.activerecord.connection import AsyncBackendGroup, BackendGroup

from rhosocial.stateflow import (
    AsyncOrderFactory,
    AsyncOrderService,
    ConcurrentStateTransitionError,
    SyncOrderFactory,
    SyncOrderService,
    async_create_tables,
    async_drop_tables,
    create_tables,
    drop_tables,
)
from rhosocial.stateflow.applications import SeatBookingFlow
from rhosocial.stateflow.applications.external_services import (
    AsyncMockPaymentService,
    MockPaymentService,
)

SYNC_MODELS = list(SeatBookingFlow.models)
ASYNC_MODELS = list(SeatBookingFlow.async_models)

PRICE = 8800

_MYSQL = {
    "host": "localhost",
    "port": 14681,
    "db": "test_db",
    "user": "root",
    "pwd": "password",
}


def _sqlite_cfg(db_path):
    return SQLiteConnectionConfig(database=db_path)


def _mysql_cfg():
    return MySQLConnectionConfig(
        host=_MYSQL["host"], port=_MYSQL["port"], database=_MYSQL["db"],
        username=_MYSQL["user"], password=_MYSQL["pwd"], charset="utf8mb4",
        autocommit=True, ssl_disabled=False,
    )


def sync_setup(backend: str, db_path: str):
    if backend == "mysql":
        config = _mysql_cfg()
        group = BackendGroup(
            name="bench-sync", models=SYNC_MODELS, config=config, backend_class=MySQLBackend
        )
    else:
        config = _sqlite_cfg(db_path)
        group = BackendGroup(
            name="bench-sync", models=SYNC_MODELS, config=config, backend_class=SQLiteBackend
        )
    group.configure()
    b = group.get_backend()
    b.connect()
    b.introspect_and_adapt()
    create_tables(b)
    template, steps = SeatBookingFlow.build_template()
    template.save()
    for s in steps:
        s.save()
    return group, (template, steps)


def sync_teardown(group):
    b = group.get_backend()
    try:
        drop_tables(b)
    except Exception:
        pass
    try:
        group.disconnect()
    except Exception:
        pass


async def async_setup(backend: str, db_path: str):
    if backend == "mysql":
        config = _mysql_cfg()
        group = AsyncBackendGroup(
            name="bench-async", models=ASYNC_MODELS, config=config, backend_class=AsyncMySQLBackend
        )
    else:
        config = _sqlite_cfg(db_path)
        group = AsyncBackendGroup(
            name="bench-async", models=ASYNC_MODELS, config=config, backend_class=AsyncSQLiteBackend
        )
    await group.configure()
    b = group.get_backend()
    await b.connect()
    await b.introspect_and_adapt()
    await async_create_tables(b)
    template, steps = SeatBookingFlow.build_async_template()
    await template.save()
    for s in steps:
        await s.save()
    return group, (template, steps)


async def async_teardown(group):
    b = group.get_backend()
    try:
        await async_drop_tables(b)
    except Exception:
        pass
    try:
        await group.disconnect()
    except Exception:
        pass


def _sync_persist(instance):
    instance.order.save()
    instance.process.save()
    for sp in instance.subprocesses:
        sp.save()
    for d in instance.dependencies:
        d.save()
    for e in instance.events:
        e.save()


async def _async_persist(instance):
    await instance.order.save()
    await instance.process.save()
    for sp in instance.subprocesses:
        await sp.save()
    for d in instance.dependencies:
        await d.save()
    for e in instance.events:
        await e.save()


def sync_one_order(template, steps, idx, payment):
    t = {}
    t0 = time.perf_counter()
    instance = SyncOrderFactory().create(
        template, steps, context={"seat_id": f"A-{idx}", "price": PRICE, "event": "bench"}
    )
    t["factory"] = time.perf_counter() - t0

    for sp in instance.subprocesses:
        sp.extra = {"seat_id": f"A-{idx}", "price": PRICE}
        sp.save()
    t0 = time.perf_counter()
    _sync_persist(instance)
    t["persist"] = time.perf_counter() - t0

    svc = SyncOrderService()
    order_id = instance.order.id
    pay_sp = instance.get_subprocess("payment")
    tx_id = payment.charge(order_id, PRICE)
    pay_sp.extra["tx_id"] = tx_id
    pay_sp.save()

    ev = f"o{idx}"
    timings = []
    for label, sp_name, status in [
        ("pub_select", "select_seat", "seat_selected"),
        ("pub_validate", "validate", "validated"),
        ("pub_payment", None, "paid"),
        ("pub_ticket", "issue_ticket", "ticketed"),
    ]:
        sp_id = pay_sp.id if sp_name is None else instance.get_subprocess(sp_name).id
        t0 = time.perf_counter()
        svc.publish_event(
            order_id=order_id, subprocess_id=sp_id, new_status=status, event_key=f"{label}-{ev}"
        )
        timings.append(time.perf_counter() - t0)
    t["publish_sum"] = sum(timings)
    t["publish_each"] = timings
    return t


async def async_one_order(template, steps, idx, payment):
    t0 = time.perf_counter()
    instance = await AsyncOrderFactory().create(
        template, steps, context={"seat_id": f"A-{idx}", "price": PRICE, "event": "bench"}
    )
    for sp in instance.subprocesses:
        sp.extra = {"seat_id": f"A-{idx}", "price": PRICE}
        await sp.save()
    await _async_persist(instance)

    svc = AsyncOrderService()
    order_id = instance.order.id
    pay_sp = instance.get_subprocess("payment")
    tx_id = await payment.charge(order_id, PRICE)
    pay_sp.extra["tx_id"] = tx_id
    await pay_sp.save()

    ev = f"o{idx}"
    for label, sp_name, status in [
        ("pub_select", "select_seat", "seat_selected"),
        ("pub_validate", "validate", "validated"),
        ("pub_payment", None, "paid"),
        ("pub_ticket", "issue_ticket", "ticketed"),
    ]:
        sp_id = pay_sp.id if sp_name is None else instance.get_subprocess(sp_name).id
        await svc.publish_event(
            order_id=order_id, subprocess_id=sp_id, new_status=status, event_key=f"{label}-{ev}"
        )
    return time.perf_counter() - t0


def run_sync_serial(backend: str, db_path: str, n: int):
    print(f"\n[1] 串行基线 (sync, {n} orders, backend={backend})")
    group, (template, steps) = sync_setup(backend, db_path)
    payment = MockPaymentService()
    try:
        agg = {"factory": 0.0, "persist": 0.0, "publish_sum": 0.0, "publish_each": [0.0] * 4}
        t_start = time.perf_counter()
        for i in range(n):
            r = sync_one_order(template, steps, i, payment)
            agg["factory"] += r["factory"]
            agg["persist"] += r["persist"]
            agg["publish_sum"] += r["publish_sum"]
            for k, v in enumerate(r["publish_each"]):
                agg["publish_each"][k] += v
        elapsed = time.perf_counter() - t_start
        total = sum([agg["factory"], agg["persist"], agg["publish_sum"]])
        print(f"    总耗时 {elapsed*1000:.1f} ms | 串行 TPS {n/elapsed:.1f}")
        print(f"    单订单均值 {elapsed/n*1000:.2f} ms,耗时分解(占比):")
        for key, label in [("factory", "factory.create"), ("persist", "persist(~10 save)"),
                           ("publish_sum", "4x publish_event")]:
            ms = agg[key] / n * 1000
            pct = agg[key] / total * 100 if total else 0
            print(f"      {label:20s} {ms:7.2f} ms  ({pct:5.1f}%)")
        labels = ["select_seat", "validate", "payment", "issue_ticket"]
        print("    publish_event 分步均值:")
        for k, lab in enumerate(labels):
            ms = agg["publish_each"][k] / n * 1000
            print(f"      {lab:14s} {ms:7.2f} ms")
        return {"elapsed": elapsed, "tps": n / elapsed, "n": n}
    finally:
        sync_teardown(group)


async def run_async_concurrency(backend: str, db_path: str, n: int, concurrencies):
    print(f"\n[2] async 并发 (backend={backend}, orders/轮={n})")
    results = []
    for c in concurrencies:
        try:
            group, (template, steps) = await async_setup(backend, db_path)
        except Exception as e:
            print(f"    并发={c:>4d}  setup 失败: {type(e).__name__}: {str(e)[:50]}")
            continue
        payment = AsyncMockPaymentService()
        try:
            sem = asyncio.Semaphore(c)

            async def worker(i):
                async with sem:
                    return await async_one_order(template, steps, i, payment)

            t0 = time.perf_counter()
            try:
                latencies = await asyncio.wait_for(
                    asyncio.gather(*[worker(i) for i in range(n)]), timeout=60.0
                )
            except asyncio.TimeoutError:
                print(f"    并发={c:>4d}  超时(>60s):单连接无法支撑并发事务")
                results.append({"c": c, "tps": 0.0, "timeout": True})
                continue
            except Exception as e:
                print(
                    f"    并发={c:>4d}  {type(e).__name__}: {str(e)[:55]}"
                    f" -> 单连接不支持并发事务"
                )
                results.append({"c": c, "tps": 0.0, "error": type(e).__name__})
                continue
            elapsed = time.perf_counter() - t0
            latencies = sorted(latencies)
            p50 = latencies[int(n * 0.50)]
            p95 = latencies[min(int(n * 0.95), n - 1)]
            p99 = latencies[min(int(n * 0.99), n - 1)]
            tps = n / elapsed
            print(
                f"    并发={c:>4d}  TPS={tps:7.1f}  "
                f"延迟 ms: P50={p50*1000:7.1f}  P95={p95*1000:7.1f}  P99={p99*1000:7.1f}  "
                f"总耗={elapsed*1000:8.1f}ms"
            )
            results.append({"c": c, "tps": tps, "p50": p50, "p95": p95, "p99": p99})
        finally:
            try:
                await async_teardown(group)
            except Exception:
                pass
    return results


async def run_conflict_storm(backend: str, db_path: str, n_orders: int, c: int):
    print(f"\n[3] 乐观锁冲突风暴 (backend={backend}, {n_orders} orders 各 {c} 并发 publish)")
    try:
        group, (template, steps) = await async_setup(backend, db_path)
    except Exception as e:
        print(f"    setup 失败: {type(e).__name__}: {str(e)[:50]}")
        return
    payment = AsyncMockPaymentService()
    try:
        total_attempts = 0
        conflicts = 0
        successes = 0
        errors = 0
        for oi in range(n_orders):
            instance = await AsyncOrderFactory().create(
                template, steps,
                context={"seat_id": f"A-{oi}", "price": PRICE, "event": "bench"},
            )
            for sp in instance.subprocesses:
                sp.extra = {"seat_id": f"A-{oi}", "price": PRICE}
                await sp.save()
            await _async_persist(instance)
            svc = AsyncOrderService()
            order_id = instance.order.id
            sp_id = instance.get_subprocess("select_seat").id

            async def attempt(i, oi=oi):
                nonlocal total_attempts, conflicts, successes, errors
                total_attempts += 1
                try:
                    await svc.publish_event(
                        order_id=order_id, subprocess_id=sp_id,
                        new_status="seat_selected", event_key=f"storm-{oi}-{i}",
                    )
                    successes += 1
                except ConcurrentStateTransitionError:
                    conflicts += 1
                except Exception:
                    errors += 1

            try:
                await asyncio.wait_for(
                    asyncio.gather(*[attempt(i) for i in range(c)]), timeout=30.0
                )
            except asyncio.TimeoutError:
                print(f"    超时(>30s):单连接并发事务冲突,无法制造乐观锁冲突窗口")
                print(f"    -> 冲突率数据需多进程基线")
                return
            except Exception as e:
                print(
                    f"    {type(e).__name__}: 单连接无法制造并发冲突窗口; "
                    f"冲突率数据需多进程基线"
                )
                return
        print(
            f"    总尝试={total_attempts}  成功={successes}  "
            f"乐观锁冲突={conflicts}  其他错误={errors}"
        )
        if conflicts == 0:
            print(f"    -> 单连接串行执行规避了乐观锁冲突(冲突=0);真冲突需多进程")
        return {
            "attempts": total_attempts, "successes": successes,
            "conflicts": conflicts, "errors": errors,
        }
    finally:
        try:
            await async_teardown(group)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="stateflow 直接接流压力基准")
    ap.add_argument("--backend", choices=["sqlite", "mysql"], default="sqlite")
    ap.add_argument("--db", type=str, default=":memory:", help="SQLite 路径(mysql 忽略)")
    ap.add_argument("--sync-orders", type=int, default=100)
    ap.add_argument("--async-orders", type=int, default=50)
    ap.add_argument("--concurrency", type=str, default="1,4,16")
    ap.add_argument("--storm-orders", type=int, default=5)
    ap.add_argument("--storm-c", type=int, default=16)
    ap.add_argument("--mysql-host", type=str, default=_MYSQL["host"])
    ap.add_argument("--mysql-port", type=int, default=_MYSQL["port"])
    args = ap.parse_args()

    if args.backend == "mysql":
        _MYSQL["host"] = args.mysql_host
        _MYSQL["port"] = args.mysql_port

    db = args.db if args.backend == "sqlite" else "(mysql)"

    print("=" * 72)
    print(f"stateflow 直接接流压力基准 (backend={args.backend})")
    print(f"db={db}  mysql={_MYSQL['host']}:{_MYSQL['port']}/{_MYSQL['db']}  python={sys.version.split()[0]}")
    print("=" * 72)

    run_sync_serial(args.backend, args.db, args.sync_orders)

    concurrencies = [int(x) for x in args.concurrency.split(",")]
    asyncio.run(run_async_concurrency(args.backend, args.db, args.async_orders, concurrencies))

    asyncio.run(run_conflict_storm(args.backend, args.db, args.storm_orders, args.storm_c))

    print("\n" + "=" * 72)
    print("基线结论见输出上方")
    print("=" * 72)


if __name__ == "__main__":
    main()
