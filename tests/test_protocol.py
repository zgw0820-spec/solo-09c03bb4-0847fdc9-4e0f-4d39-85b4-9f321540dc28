"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against the ephemeral PostgreSQL and
concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    PoolAcquireCancelled,
    PoolAcquireTimeout,
    QueryTimeout,
)
from app.executor import QueryExecutor  # noqa: E402
from app.pool import Priority, QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="proto-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)

    print("=== D. 紧急优先与连续两条后放行普通 ===")
    # Both backends busy so every subsequent request really queues.
    async def slow(tag: str, deadline_s: float = 6) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(1.5)", time.monotonic() + deadline_s
        )

    busy = [
        asyncio.ensure_future(slow("B1")),
        asyncio.ensure_future(slow("B2")),
    ]
    await asyncio.sleep(0.3)
    grant_before = len(pool.grant_history)

    # Enqueue strictly in this order (same urgency => strict FIFO by seq):
    # N1, N2, U1, U2, U3, U4, N3
    n1 = asyncio.ensure_future(slow("N1"))
    await asyncio.sleep(0.01)
    n2 = asyncio.ensure_future(slow("N2"))
    await asyncio.sleep(0.01)
    u1 = asyncio.ensure_future(
        executor.execute(
            "SELECT 'U1', pg_sleep(0.05)",
            time.monotonic() + 8,
            priority=Priority.URGENT,
        )
    )
    await asyncio.sleep(0.01)
    u2 = asyncio.ensure_future(
        executor.execute(
            "SELECT 'U2', pg_sleep(0.05)",
            time.monotonic() + 8,
            priority=Priority.URGENT,
        )
    )
    await asyncio.sleep(0.01)
    u3 = asyncio.ensure_future(
        executor.execute(
            "SELECT 'U3', pg_sleep(0.05)",
            time.monotonic() + 8,
            priority=Priority.URGENT,
        )
    )
    await asyncio.sleep(0.01)
    u4 = asyncio.ensure_future(
        executor.execute(
            "SELECT 'U4', pg_sleep(0.05)",
            time.monotonic() + 8,
            priority=Priority.URGENT,
        )
    )
    await asyncio.sleep(0.01)
    n3 = asyncio.ensure_future(slow("N3"))
    await asyncio.sleep(0.1)

    st = await pool.status()
    check(
        "队列中 3 普通 4 紧急",
        st.waiting_normal == 3 and st.waiting_urgent == 4,
        st,
    )

    all_results = await asyncio.gather(
        *busy, n1, n2, u1, u2, u3, u4, n3
    )
    # First two grants are the two backfill waves; the queued grants follow.
    queued_grants = [p for _, p in pool.grant_history[grant_before:]]
    check(
        "放行顺序 U1,U2,N1,U3,U4,N2,N3（紧急越队+两条后让普通）",
        queued_grants
        == [
            "urgent",
            "urgent",
            "normal",
            "urgent",
            "urgent",
            "normal",
            "normal",
        ],
        queued_grants,
    )
    markers = [r["rows"][0][0] for r in all_results]
    check(
        "每个请求都拿到自身结果（无串结果）",
        sorted(markers)
        == sorted(["B1", "B2", "N1", "N2", "N3", "U1", "U2", "U3", "U4"]),
        markers,
    )
    st = await pool.status()
    check("调度结束后连接全部空闲、容量仍为 2",
          st.idle == 2 and st.size == 2 and st.waiting_normal == 0
          and st.waiting_urgent == 0 and st.urgent_grants_in_a_row == 0, st)

    print("=== E. 等待期间到期的请求不占名额、不发送语句 ===")
    busy = [
        asyncio.ensure_future(slow("E-B1")),
        asyncio.ensure_future(slow("E-B2")),
    ]
    await asyncio.sleep(0.3)
    grant_before = len(pool.grant_history)

    # Normal request with a short total budget: it must expire while queued.
    expired_err = None
    try:
        await executor.execute(
            "SELECT 'EXPIRED-NEVER-RUNS'", time.monotonic() + 0.3
        )
    except PoolAcquireTimeout as exc:
        expired_err = exc
    check("排队过期抛 PoolAcquireTimeout", expired_err is not None)

    # Urgent request with ample budget must still be granted once a slot
    # frees (the expired normal request must not have consumed the slot).
    urgent_result = await executor.execute(
        "SELECT 'E-URGENT'",
        time.monotonic() + 8,
        priority=Priority.URGENT,
    )
    check("紧急请求在普通请求过期后仍获得连接",
          urgent_result["rows"] == [["E-URGENT"]], urgent_result)
    grants_after = [p for _, p in pool.grant_history[grant_before:]]
    check("过期普通请求未产生任何放行记录", grants_after == ["urgent"],
          grants_after)

    busy_results = await asyncio.gather(*busy)
    check("占池查询各自成功",
          {r["rows"][0][0] for r in busy_results} == {"E-B1", "E-B2"},
          busy_results)
    follow_up = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("后续请求仍得到自身结果", follow_up["rows"] == [[1]], follow_up)
    st = await pool.status()
    check("池恢复满且空闲", st.idle == 2 and st.size == 2, st)

    print("=== F. 排队期间撤销的请求不占名额、不发送语句 ===")
    busy = [
        asyncio.ensure_future(slow("F-B1")),
        asyncio.ensure_future(slow("F-B2")),
    ]
    await asyncio.sleep(0.3)
    grant_before = len(pool.grant_history)

    revoke = asyncio.Event()
    cancelled_err = None
    cancelled_task = asyncio.ensure_future(
        executor.execute(
            "SELECT 'REVOKED-NEVER-RUNS'",
            time.monotonic() + 8,
            priority=Priority.NORMAL,
            cancel_event=revoke,
        )
    )
    await asyncio.sleep(0.2)
    st = await pool.status()
    check("撤销前请求在普通队列中等待", st.waiting_normal == 1, st)
    revoke.set()
    try:
        await cancelled_task
    except PoolAcquireCancelled as exc:
        cancelled_err = exc
    check("撤销排队中的请求抛 PoolAcquireCancelled", cancelled_err is not None)

    # New urgent request must get the next freed slot; the revoked one never
    # appears in grant history and never sent SQL.
    urgent_result = await executor.execute(
        "SELECT 'F-URGENT'",
        time.monotonic() + 8,
        priority=Priority.URGENT,
    )
    check("撤销后紧急请求正常获得连接",
          urgent_result["rows"] == [["F-URGENT"]], urgent_result)
    grants_after = [p for _, p in pool.grant_history[grant_before:]]
    check("被撤销请求未产生任何放行记录", grants_after == ["urgent"],
          grants_after)
    await asyncio.gather(*busy)
    follow_up = await executor.execute("SELECT 2", time.monotonic() + 5)
    check("接续查询得到自身结果", follow_up["rows"] == [[2]], follow_up)
    st = await pool.status()
    check("池恢复满且空闲、无残留等待",
          st.idle == 2 and st.size == 2 and st.waiting_normal == 0
          and st.waiting_urgent == 0, st)

    await pool.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
