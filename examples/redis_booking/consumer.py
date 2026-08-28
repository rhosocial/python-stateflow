# examples/redis_booking/consumer.py
"""消费者:stateflow worker。BRPOP 串行拉取 redis 队列请求 -> 处理 -> 结果回写。

stateflow 单实例只能串行访问(activerecord 非线程安全),正适合做串行消费者。
横向扩展:启多个 worker 进程,各从同一队列 BRPOP(redis 自动分发,互不重复)。
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (  # noqa: E402
    DEFAULT_EVENT,
    QUEUE_KEY,
    RESULT_PREFIX,
    RESULT_TTL,
    STATUS_PREFIX,
    deduct_stock,
    get_redis,
    process_request,
    setup_stateflow,
    teardown_stateflow,
)


def run_worker(max_jobs=None, timeout=5, stock_event=None, stock_total=None):
    r = get_redis()
    group, ctx = setup_stateflow(stock_event=stock_event, stock_total=stock_total)
    backend = group.get_backend()
    stats = {"done": 0, "no_ticket": 0, "failed": 0}
    try:
        while True:
            item = r.brpop(QUEUE_KEY, timeout=timeout)
            if item is None:
                if max_jobs and sum(stats.values()) >= max_jobs:
                    break
                continue
            payload = json.loads(item[1])
            rid = payload["request_id"]
            if stock_event and not deduct_stock(backend, stock_event):
                r.set(f"{STATUS_PREFIX}{rid}", "no_ticket", ex=RESULT_TTL)
                stats["no_ticket"] += 1
            else:
                try:
                    res = process_request(ctx, payload)
                    r.set(f"{RESULT_PREFIX}{rid}", json.dumps(res), ex=RESULT_TTL)
                    r.set(f"{STATUS_PREFIX}{rid}", "done", ex=RESULT_TTL)
                    stats["done"] += 1
                except Exception as e:
                    r.set(
                        f"{STATUS_PREFIX}{rid}",
                        f"failed:{type(e).__name__}",
                        ex=RESULT_TTL,
                    )
                    stats["failed"] += 1
            if max_jobs and sum(stats.values()) >= max_jobs:
                break
    finally:
        teardown_stateflow(group)
        r.close()
    return stats


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    stock = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if n:
        print(f"消费 {n} 个请求后退出" + (f"(余票={stock})" if stock else ""))
    else:
        print("stateflow consumer 启动,等待 redis 请求...(Ctrl+C 退出)")
    stats = run_worker(max_jobs=n or None, stock_event=DEFAULT_EVENT if stock else None,
                       stock_total=stock)
    print(f"结果: {stats}")
