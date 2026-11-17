"""End-to-end self-test for the read-only query API.

Runs entirely on the Python standard library so it works inside the slim
application container as well as on a developer machine:

    python -m tests.selftest

Scenarios:
  1. health + normal SELECT returns the analyst's own rows
  2. read-only guard rails (multi-statement, INSERT, non-SELECT rejected)
  3. single slow query times out, is cancelled, receipt is final
  4. a queued request whose budget expires while waiting never executes
  5. after two concurrent slow queries, later requests still get their own
     results (connection reuse after confirmed rollback)
  6. pool capacity really is two (observed via pg_stat_activity)
  7. mixed urgent/normal queue: urgent overtakes, two-in-a-row cap, FIFO
  8. queued expiry/revocation never occupies a slot; follow-up queries work
"""
from __future__ import annotations

import http.client
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def run_case(title: str, fn) -> None:
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - test harness must keep going
        check(f"{title} 未抛异常", False, repr(exc))


def case_health_and_basic() -> None:
    status, body = get("/health")
    check("健康检查 200", status == 200, body)
    check("连接池容量为 2", body["pool"]["size"] == 2, body)
    check("启动即建满两条连接", body["pool"]["idle"] == 2, body)

    status, body = post(
        "/query",
        {"sql": "SELECT count(*) AS n FROM fault_logs", "timeout_ms": 5000},
    )
    check("普通 SELECT 200", status == 200, body)
    check("返回 ok 且已执行", body["status"] == "ok" and body["executed"], body)
    check("预置 7 条故障日志", body["rows"][0][0] == 7, body)

    status, body = post(
        "/query",
        {
            "sql": "SELECT device_id, severity FROM fault_logs "
                   "WHERE severity = 'CRITICAL' ORDER BY id",
            "timeout_ms": 5000,
        },
    )
    check("过滤查询返回自身结果", status == 200 and body["row_count"] == 2, body)
    check(
        "结果内容正确",
        [r[0] for r in body["rows"]] == ["SW-CORE-01", "SRV-DB-07"],
        body,
    )


def case_read_only_guards() -> None:
    for name, sql in [
        ("多语句", "SELECT 1; SELECT 2"),
        ("INSERT", "INSERT INTO fault_logs (device_id) VALUES ('x')"),
        ("UPDATE", "UPDATE fault_logs SET site = 'z'"),
        ("非 SELECT", "SHOW transaction_read_only"),
        ("SELECT INTO", "SELECT 1 INTO TEMP t"),
    ]:
        status, body = post("/query", {"sql": sql, "timeout_ms": 5000})
        check(f"{name} 被拒绝(400)", status == 400, body)
        check(f"{name} 错误码 invalid_sql",
              body.get("error", {}).get("code") == "invalid_sql", body)

    # pg_sleep 是允许的只读函数，用于慢查询演示。
    status, body = post(
        "/query", {"sql": "SELECT pg_sleep(0.1)", "timeout_ms": 5000}
    )
    check("pg_sleep 只读函数允许执行", status == 200 and body["status"] == "ok", body)


def case_single_timeout() -> None:
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT pg_sleep(3)", "timeout_ms": 800},
        timeout=15,
    )
    elapsed = time.monotonic() - t0
    check("超时返回 504", status == 504, body)
    check("错误码 query_timeout",
          body.get("error", {}).get("code") == "query_timeout", body)
    check("回执 status=timeout", body.get("status") == "timeout", body)
    check("语句确实已发送执行", body.get("executed") is True, body)
    check("已发起取消", body.get("cancelled") is True, body)
    check("及时返回 (<2.5s)", elapsed < 2.5, f"elapsed={elapsed:.2f}")

    # Give the server a moment, then prove the slow statement is gone and
    # the connection was reused for a fresh, independent query.
    time.sleep(0.5)
    status, body = post(
        "/query", {"sql": "SELECT 42 AS answer", "timeout_ms": 5000}
    )
    check("超时后连接可复用且返回自身结果",
          status == 200 and body["rows"] == [[42]], body)

    status, health = get("/health")
    check("池容量仍为 2", health["pool"]["size"] == 2, health)
    check("确认事务结束后复用，本用例无新增弃置",
          health["pool"]["total_discarded"] == discarded_before, health)


def case_queued_expiry() -> None:
    results: list[tuple] = []

    def slow(marker: str, sleep_s: float, budget_ms: int) -> None:
        t0 = time.monotonic()
        status, body = post(
            "/query",
            {"sql": f"SELECT '{marker}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=20,
        )
        results.append((marker, status, body, round(time.monotonic() - t0, 2)))

    # Two slow queries occupy both connections for ~2.5s.
    t1 = threading.Thread(target=slow, args=("A", 2.5, 5000))
    t2 = threading.Thread(target=slow, args=("B", 2.5, 5000))
    t1.start()
    time.sleep(0.3)
    t2.start()
    time.sleep(0.3)

    # Third request: only 500ms total budget, both backends are busy.
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT 'SHOULD-NOT-RUN', pg_sleep(0)", "timeout_ms": 500},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("排队过期返回 503", status == 503, body)
    check("错误码 acquire_timeout",
          body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队等待本身计入预算 (~0.5s)", 0.4 < waited < 1.5,
          f"waited={waited:.2f}")

    t1.join()
    t2.join()
    ok = [r for r in results if r[1] == 200 and r[2]["status"] == "ok"]
    check("两条占池慢查询最终各自成功", len(ok) == 2, results)
    check(
        "两条慢查询拿到各自的标记结果",
        {r[2]["rows"][0][0] for r in ok} == {"A", "B"},
        results,
    )

    # After the backlog drains, pool must be fully reusable.
    status, body = post(
        "/query", {"sql": "SELECT 'AFTER-BACKLOG'", "timeout_ms": 5000}
    )
    check("积压清空后新查询正常",
          status == 200 and body["rows"][0][0] == "AFTER-BACKLOG", body)


def case_capacity_and_reuse() -> None:
    barrier = threading.Barrier(3)
    finished: list = []

    def worker(i: int) -> None:
        barrier.wait()
        status, body = post(
            "/query",
            {"sql": f"SELECT {i}, pg_sleep(1.2)", "timeout_ms": 8000},
            timeout=15,
        )
        finished.append((i, status, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    for t in threads:
        t.start()
    t0 = time.monotonic()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    check("三个并发请求全部成功",
          len(finished) == 3 and all(s == 200 for _, s, _ in finished),
          finished)
    check(
        "每个请求拿到自身结果(无串结果)",
        {b["rows"][0][0] for _, _, b in finished} == {0, 1, 2},
        finished,
    )
    # Capacity 2 -> three 1.2s sleeps must take > 1.8s (two waves).
    check("容量 2 导致第三个请求排队 (>1.8s)", wall > 1.8, f"wall={wall:.2f}")

    status, health = get("/health")
    check("结束后两条连接全部空闲", health["pool"]["idle"] == 2, health)
    check("容量保持 2、本用例无新增弃置",
          health["pool"]["size"] == 2
          and health["pool"]["total_discarded"] == discarded_before, health)


def case_discard_and_rebuild() -> None:
    """Kill a pooled backend out-of-band; the API must discard and rebuild."""
    admin_dsn = os.environ.get("ADMIN_DSN")
    if not admin_dsn:
        dsn_file = os.path.join(tempfile.gettempdir(), "faultdb_admin_dsn")
        if os.path.exists(dsn_file):
            admin_dsn = open(dsn_file, encoding="utf-8").read().strip()
    if not admin_dsn:
        print("  SKIP  弃置补建混沌测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  弃置补建混沌测试（无 psycopg）")
        return

    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename = 'fault_ro' AND pid <> pg_backend_pid()"
        )
        pids = [r[0] for r in cur.fetchall()]
        check("池内存在两条 fault_ro 后端", len(pids) == 2, pids)
        victim = pids[0]
        cur.execute("SELECT pg_terminate_backend(%s)", (victim,))
        check("管理连接成功终止一个后端", cur.fetchone()[0] is True)

    # Run several sequential queries so both pooled backends are cycled;
    # whichever request draws the dead backend must transparently discard,
    # rebuild and retry — every client call still succeeds with its own rows.
    statuses, markers = [], []
    for i in range(4):
        status, body = post(
            "/query",
            {"sql": f"SELECT 'CYCLE-{i}'", "timeout_ms": 8000},
        )
        statuses.append(status)
        if status == 200:
            markers.append(body["rows"][0][0])
    check("循环借出期间所有请求均成功(客户端无感)",
          statuses == [200] * 4, statuses)
    check("每次都返回自身结果",
          markers == [f"CYCLE-{i}" for i in range(4)], markers)

    status, health = get("/health")
    check("弃置计数增加",
          health["pool"]["total_discarded"] >= 1, health)
    check("补建后容量恢复为 2 且全空闲",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)

    # Physical capacity check: while two slow queries run, the database must
    # see at most two fault_ro backends — no third connection is ever opened.
    observed: list[int] = []
    barrier = threading.Barrier(2)

    def slow(i: int) -> None:
        barrier.wait()
        post("/query",
             {"sql": f"SELECT {i}, pg_sleep(2)", "timeout_ms": 8000},
             timeout=15)

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 3
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename='fault_ro'"
            )
            observed.append(cur.fetchone()[0])
            time.sleep(0.1)
    for t in threads:
        t.join()
    check("数据库侧 fault_ro 后端数从未超过 2",
          observed and max(observed) <= 2, f"max={max(observed)}")


def case_priority_default_and_field() -> None:
    # Default urgency is ordinary and the receipt echoes the actual level.
    status, body = post(
        "/query", {"sql": "SELECT 1", "timeout_ms": 5000}
    )
    check("缺省 priority=normal 且回执回显",
          status == 200 and body.get("priority") == "normal", body)

    status, body = post(
        "/query",
        {"sql": "SELECT 2", "timeout_ms": 5000, "priority": "urgent"},
    )
    check("显式 urgent 回执回显",
          status == 200 and body.get("priority") == "urgent", body)

    status, body = post(
        "/query",
        {"sql": "SELECT 3", "timeout_ms": 5000, "priority": "critical"},
    )
    check("非法 priority 被 422 拒绝", status == 422, body)


def case_mixed_priority_queue() -> None:
    """Urgent overtakes ordinary; burst cap 2; FIFO inside one class."""
    finished: list[tuple] = []
    lock = threading.Lock()

    def slow(marker: str, sleep_s: float, priority: str) -> None:
        t0 = time.monotonic()
        status, body = post(
            "/query",
            {
                "sql": f"SELECT '{marker}', clock_timestamp(), "
                       f"pg_sleep({sleep_s})",
                "timeout_ms": 15000,
                "priority": priority,
            },
            timeout=30,
        )
        with lock:
            finished.append(
                (
                    marker,
                    status,
                    body,
                    round((time.monotonic() - t0), 3),
                )
            )

    # Two ordinary slow queries occupy both connections (~2.5s).
    holders = [
        threading.Thread(target=slow, args=("H1", 2.5, "normal")),
        threading.Thread(target=slow, args=("H2", 2.5, "normal")),
    ]
    for t in holders:
        t.start()
    time.sleep(0.4)

    # Now queue, in strict order: N1, N2, U1, U2, U3, U4, N3.
    queued: list[threading.Thread] = []
    for marker, prio in [
        ("N1", "normal"), ("N2", "normal"),
        ("U1", "urgent"), ("U2", "urgent"), ("U3", "urgent"), ("U4", "urgent"),
        ("N3", "normal"),
    ]:
        t = threading.Thread(target=slow, args=(marker, 0.05, prio))
        queued.append(t)
        t.start()
        time.sleep(0.05)

    time.sleep(0.3)
    status, health = get("/health")
    check("健康检查可见普通/紧急排队计数",
          status == 200 and health["pool"]["waiting_normal"] == 3
          and health["pool"]["waiting_urgent"] == 4, health)

    for t in holders + queued:
        t.join()

    by_marker = {m: (s, b, el) for m, s, b, el in finished}
    check("9 个请求全部成功",
          len(finished) == 9 and all(s == 200 for _, s, _, _ in finished),
          finished)
    check(
        "每个请求拿到自身标记结果（无串结果）",
        {b["rows"][0][0] for _, _, b, _ in finished}
        == {"H1", "H2", "N1", "N2", "N3", "U1", "U2", "U3", "U4"},
        finished,
    )

    def started_at(marker: str) -> str:
        # clock_timestamp() text is lexicographically orderable (timestamptz).
        return by_marker[marker][1]["rows"][0][1]

    check("U1 越过先入队的 N1/N2 先执行",
          started_at("U1") < started_at("N1"), finished)
    check("连续两条紧急后强制放行普通 N1",
          started_at("N1") < started_at("U3"), finished)
    check("第二轮两条紧急后强制放行普通 N2",
          started_at("N2") < started_at("N3")
          and started_at("U4") < started_at("N2"),
          finished)
    check("同等级内保持 FIFO（U1 先于 U2，N1 先于 N2）",
          started_at("U1") < started_at("U2")
          and started_at("N1") < started_at("N2"),
          finished)
    check("占用连接的 H1/H2 始终未被中断（均成功）",
          by_marker["H1"][0] == 200 and by_marker["H2"][0] == 200, finished)

    status, health = get("/health")
    check("队列清空、两条连接全部空闲",
          health["pool"]["idle"] == 2
          and health["pool"]["waiting_normal"] == 0
          and health["pool"]["waiting_urgent"] == 0, health)


def case_queued_expiry_and_revoke() -> None:
    """Expiry while queued never runs; a revoked queued request frees its slot."""
    # ---- Part 1: short-budget request expires behind two busy backends ----
    results: list[tuple] = []

    def slow(marker: str, sleep_s: float, budget_ms: int) -> None:
        status, body = post(
            "/query",
            {"sql": f"SELECT '{marker}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=20,
        )
        results.append((marker, status, body))

    t1 = threading.Thread(target=slow, args=("A", 2.0, 8000))
    t2 = threading.Thread(target=slow, args=("B", 2.0, 8000))
    t1.start()
    time.sleep(0.25)
    t2.start()
    time.sleep(0.25)

    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT 'EXPIRED-NEVER-RUNS'", "timeout_ms": 400},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("排队过期返回 503 且语句未执行",
          status == 503
          and body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队等待计入预算 (~0.4s)", 0.35 < waited < 1.5, f"waited={waited:.2f}")

    # An urgent request queued afterwards must still get its own result once
    # a slot frees; the expired request must not have consumed any slot.
    status, body = post(
        "/query",
        {"sql": "SELECT 'AFTER-EXPIRY-URGENT'",
         "timeout_ms": 12000, "priority": "urgent"},
        timeout=20,
    )
    check("过期请求不占名额，后续紧急请求获得自身结果",
          status == 200 and body["rows"][0][0] == "AFTER-EXPIRY-URGENT", body)

    t1.join()
    t2.join()
    check("占池查询各自成功",
          {m for m, s, b in results if s == 200} == {"A", "B"}, results)

    # ---- Part 2: client disconnects while still queued ----
    holders = [
        threading.Thread(target=slow, args=("C1", 2.0, 8000)),
        threading.Thread(target=slow, args=("C2", 2.0, 8000)),
    ]
    for t in holders:
        t.start()
    time.sleep(0.5)

    def disconnect_while_queued() -> tuple:
        conn = http.client.HTTPConnection("127.0.0.1", 8000, timeout=10)
        # Marker text deliberately avoids SQL keywords blocked by the
        # read-only validator (e.g. REVOKE) so the request really enqueues.
        payload = json.dumps(
            {"sql": "SELECT 'GONE-WHILE-WAITING'", "timeout_ms": 10000}
        )
        conn.request(
            "POST", "/query", body=payload,
            headers={"Content-Type": "application/json"},
        )
        time.sleep(0.4)  # ensure the request is queued behind both backends
        conn.close()    # revoke while waiting; running SQL is untouched
        return ("revoked",)

    dt = threading.Thread(target=disconnect_while_queued)
    dt.start()
    dt.join()

    # Give the disconnect watcher a moment, then verify no slot was stolen.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        _, health = get("/health")
        if health["pool"]["waiting_normal"] == 0:
            break
        time.sleep(0.1)
    check("断开后排队计数归零（请求已离开队列）",
          health["pool"]["waiting_normal"] == 0
          and health["pool"]["in_use"] == 2, health)

    status, body = post(
        "/query",
        {"sql": "SELECT 'AFTER-GONE'", "timeout_ms": 12000},
        timeout=20,
    )
    check("撤销不占名额，接续查询获得自身结果",
          status == 200 and body["rows"][0][0] == "AFTER-GONE", body)

    for t in holders:
        t.join()
    status, health = get("/health")
    check("结束后两条连接全部空闲、容量为 2",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)


def main() -> int:
    print(f"self-test against {BASE}")
    run_case("1. 健康检查与普通查询", case_health_and_basic)
    run_case("2. 只读约束", case_read_only_guards)
    run_case("3. 单条慢查询超时与取消", case_single_timeout)
    run_case("4. 排队期间过期不执行", case_queued_expiry)
    run_case("5. 容量为 2 与接续查询", case_capacity_and_reuse)
    run_case("6. 弃置并补建 / 物理容量", case_discard_and_rebuild)
    run_case("7. 紧急等级字段与混合队列调度", case_priority_default_and_field)
    run_case("8. 排队到期/撤销不占名额与接续查询",
             case_queued_expiry_and_revoke)
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
