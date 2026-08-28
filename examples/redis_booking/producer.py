# examples/redis_booking/producer.py
"""生产者:用户提交抢票请求 -> LPUSH 入 redis 队列 -> 立即返回登记号。

用户端体验:提交后毫秒级拿到 request_id(已登记),无需等待座位分配。
结果稍后自行查询 / 等通知 / 前端轮询(本质都是异步取结果)。
"""

import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from common import (  # noqa: E402
    QUEUE_KEY,
    RESULT_TTL,
    STATUS_PREFIX,
    get_redis,
)
import json  # noqa: E402


def submit_request(user_id, event="concert", seat_pref=None, r=None):
    close = False
    if r is None:
        r = get_redis()
        close = True
    rid = f"REQ-{uuid.uuid4().hex[:12]}"
    payload = {
        "request_id": rid,
        "user_id": user_id,
        "event": event,
        "seat_pref": seat_pref,
        "ts": time.time(),
    }
    r.lpush(QUEUE_KEY, json.dumps(payload))
    r.set(f"{STATUS_PREFIX}{rid}", "registered", ex=RESULT_TTL)
    if close:
        r.close()
    return rid


def query_status(rid, r=None):
    close = False
    if r is None:
        r = get_redis()
        close = True
    st = r.get(f"{STATUS_PREFIX}{rid}")
    res = r.get(f"stateflow:booking:result:{rid}")
    if close:
        r.close()
    return {"status": st, "result": res}


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    r = get_redis()
    t0 = time.perf_counter()
    rids = [submit_request(f"u{i}", r=r) for i in range(n)]
    dt = (time.perf_counter() - t0) * 1000
    print(f"生产 {n} 请求,登记总耗 {dt:.1f} ms,均值 {dt/n:.3f} ms/请求 (立即返回)")
    for rid in rids[:3]:
        print(f"  {rid} -> 已登记")
    r.close()
