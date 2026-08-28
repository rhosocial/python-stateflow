# examples/redis_booking/run_demo.py
"""削峰对比实验:无缓冲(直接同步处理) vs 有缓冲(redis 队列 + 串行消费)。

模拟抢票瞬时尖峰:N 个用户同时提交。
- 无缓冲:每个请求同步走 stateflow 处理,用户必须等处理完成才拿到响应。
  尖峰下用户端延迟 = 排队 + 处理(第 i 个用户等到前 i-1 个处理完),体验差。
- 有缓冲:请求 LPUSH 入 redis(毫秒级返回登记),1 个 worker 串行消费。
  用户端立即拿到登记号,处理在后台串行进行,尖峰被 redis 吸收。

两种模式 DB 侧处理总量相同(stateflow 都串行),差异在用户端响应:
无缓冲用户等到 N x 单订单耗时;有缓冲用户 ~毫秒级登记。
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import (  # noqa: E402
    DEFAULT_EVENT,
    QUEUE_KEY,
    get_redis,
    make_payload,
    process_request,
    setup_stateflow,
    teardown_stateflow,
)
from consumer import run_worker  # noqa: E402


def pct(sorted_vals, p):
    return sorted_vals[min(int(len(sorted_vals) * p), len(sorted_vals) - 1)]


def scenario_no_buffer(n):
    r = get_redis()
    r.delete(QUEUE_KEY)
    group, ctx = setup_stateflow()
    responses = []
    t0 = time.perf_counter()
    for i in range(n):
        process_request(ctx, make_payload(i))
        responses.append(time.perf_counter() - t0)
    total = time.perf_counter() - t0
    teardown_stateflow(group)
    r.close()
    return responses, total


def scenario_buffered(n):
    r = get_redis()
    r.delete(QUEUE_KEY)
    responses = []
    t0 = time.perf_counter()
    for i in range(n):
        r.lpush(QUEUE_KEY, json.dumps(make_payload(i)))
        responses.append(time.perf_counter() - t0)
    register_total = time.perf_counter() - t0

    group, ctx = setup_stateflow()
    drain_t0 = time.perf_counter()
    for _ in range(n):
        item = r.brpop(QUEUE_KEY, timeout=10)
        payload = json.loads(item[1])
        process_request(ctx, payload)
    drain_total = time.perf_counter() - drain_t0
    teardown_stateflow(group)
    r.close()
    return responses, register_total, drain_total


def scenario_shortcircuit(n, stock):
    r = get_redis()
    r.delete(QUEUE_KEY)
    for i in range(n):
        r.lpush(QUEUE_KEY, json.dumps(make_payload(i)))
    t0 = time.perf_counter()
    stats = run_worker(max_jobs=n, stock_event=DEFAULT_EVENT, stock_total=stock)
    total = time.perf_counter() - t0
    r.close()
    return stats, total


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    print("=" * 72)
    print(f"削峰对比实验:模拟 {n} 个用户瞬时抢票")
    print("=" * 72)

    resp_nb, total_nb = scenario_no_buffer(n)
    resp_nb.sort()
    p50_nb = pct(resp_nb, 0.50)
    p95_nb = pct(resp_nb, 0.95)
    max_nb = resp_nb[-1]

    resp_b, reg_total, drain_total = scenario_buffered(n)
    resp_b.sort()
    p50_b = pct(resp_b, 0.50)
    p95_b = pct(resp_b, 0.95)
    max_b = resp_b[-1]

    print(f"\n[无缓冲] 用户提交 -> 同步等 stateflow 处理完成才返回:")
    print(f"  用户端响应  P50={p50_nb*1000:8.1f}ms  P95={p95_nb*1000:8.1f}ms  "
          f"Max={max_nb*1000:8.1f}ms")
    print(f"  (第 i 个用户被迫排队等前 i-1 个处理完)")
    print(f"  处理总时 {total_nb*1000:.1f}ms")

    print(f"\n[有缓冲] 用户提交 -> LPUSH 立即返回登记号:")
    print(f"  用户端登记  P50={p50_b*1000:8.1f}ms  P95={p95_b*1000:8.1f}ms  "
          f"Max={max_b*1000:8.1f}ms")
    print(f"  (队列吸收尖峰,用户无感)")
    print(f"  登记总时 {reg_total*1000:.1f}ms | 后台消费总时 {drain_total*1000:.1f}ms")

    print(f"\n削峰效果:")
    print(f"  用户端 P95 响应: {p95_nb*1000:.1f}ms -> {p95_b*1000:.1f}ms "
          f"(降幅 {(1 - p95_b/p95_nb)*100:.1f}%)")
    print(f"  DB 侧处理总量: {total_nb*1000:.1f}ms vs {drain_total*1000:.1f}ms "
          f"(基本相同,stateflow 都串行)")
    print(f"  核心价值: 用户从'等 N 单处理'变为'立即登记',DB 被redis 平滑保护")

    stock = n * 3 // 5 if n >= 5 else max(n - 2, 1)
    stats, sc_total = scenario_shortcircuit(n, stock)
    done = stats["done"]
    no_ticket = stats["no_ticket"]
    per_order = sc_total / done if done else 0
    no_sc_total_est = n * per_order
    saved = no_sc_total_est - sc_total
    print(f"\n[余票短路] {n} 请求 / 余票 {stock}:")
    print(f"  成功出票={done}  无票短路={no_ticket}  (无票订单跳过 stateflow,直接标记)")
    print(f"  消费总时 {sc_total*1000:.1f}ms (仅 {done} 单走 stateflow)")
    print(f"  若不短路(全 {n} 单走 stateflow)约 {no_sc_total_est*1000:.1f}ms,"
          f" 省约 {saved*1000:.1f}ms ({saved/no_sc_total_est*100:.1f}%)")
    print(f"  价值: 余票归零后后续请求不进 stateflow,保护 DB 且即时告知无票")


if __name__ == "__main__":
    main()
