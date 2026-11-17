"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  When both are
  busy additional requests *wait* in an in-process priority queue instead
  of opening another backend.
* Two urgency levels are supported (see :class:`Priority`): ordinary
  requests queue FIFO, urgent requests may overtake already-queued
  ordinary ones.  Reordering only affects *waiting* requests: a statement
  already executing is never interrupted.
* Anti-starvation: while at least one ordinary request is waiting, after
  two consecutive urgent grants the next free slot must go to an ordinary
  request.  The counter resets once no ordinary request remains queued.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire` takes an absolute
  monotonic deadline).  A request whose deadline passes (or which is
  cancelled, e.g. the client disconnects) while queued never occupies a
  slot and no statement is ever sent on its behalf; later requests still
  receive their own results.
* After a timeout the handler must prove the backend is idle again
  (``transaction_status == IDLE`` after a successful ``ROLLBACK``) before
  the connection is returned to the pool.  If that cannot be confirmed the
  connection is closed and discarded, and a replacement is built so the
  pool keeps its capacity of two.
* Every connection is opened in a read-only, read-committed session.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from enum import Enum

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireCancelled, PoolAcquireTimeout

logger = logging.getLogger("querypool")


class Priority(str, Enum):
    """Request urgency. Values are the literal strings exposed in the API."""

    NORMAL = "normal"
    URGENT = "urgent"


# At most this many urgent requests may be granted in a row while an
# ordinary request is still waiting; afterwards an ordinary one goes first.
URGENT_BURST_LIMIT = 2


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool
    waiting_normal: int
    waiting_urgent: int
    urgent_grants_in_a_row: int


class _Waiter:
    """One queued acquisition request."""

    __slots__ = ("seq", "priority", "deadline", "granted")

    def __init__(self, seq: int, priority: Priority, deadline: float) -> None:
        # Monotonic insertion number -> strict FIFO within one urgency class.
        self.seq = seq
        self.priority = priority
        self.deadline = deadline
        self.granted = asyncio.Event()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"_Waiter(seq={self.seq}, priority={self.priority.value}, "
            f"deadline={self.deadline:.3f})"
        )


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Free physical backends. A borrower slot is claimed before taking
        # one of these, so a slot holder may also have to (re)build a backend.
        self._free: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        # Number of borrower slots currently held (executing or building).
        self._borrowers = 0
        # Priority wait queue; every mutation happens on the event loop and
        # is atomic between awaits, so no extra lock is required.
        self._waiting: list[_Waiter] = []
        self._next_seq = 0
        # Consecutive urgent grants while at least one normal request waits.
        self._urgent_streak = 0
        # Most recent grant decisions, oldest first: (seq, priority value).
        # Kept for tests/observability of the scheduling policy.
        self.grant_history: list[tuple[int, str]] = []
        # Number of physical backends currently tracked (idle + in use).
        # It can temporarily fall below pool_size when a rebuild fails.
        self._backends = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._lock:
            for _ in range(self._settings.pool_size):
                conn = await self._make_connection()
                await self._free.put(conn)
        logger.info(
            "pool opened: size=%d created=%d",
            self._settings.pool_size,
            self._total_created,
        )

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            while True:
                try:
                    conn = self._free.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._backends -= 1
                await self._safe_close(conn)
            # Wake every queued request; their wait_for() ends immediately.
            for waiter in self._waiting:
                waiter.granted.set()
            self._waiting.clear()
            logger.info("pool closed")

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=max(0, self._backends - idle),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
            waiting_normal=sum(
                1 for w in self._waiting if w.priority is Priority.NORMAL
            ),
            waiting_urgent=sum(
                1 for w in self._waiting if w.priority is Priority.URGENT
            ),
            urgent_grants_in_a_row=self._urgent_streak,
        )

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self) -> AsyncConnection:
        """Open a fresh connection pinned to a read-only session."""
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            self._settings.database_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        # Session-level safety net: even an accidental COMMIT cannot write.
        await conn.execute("SET default_transaction_read_only = on")
        await conn.execute(
            "SET default_transaction_isolation = 'read committed'"
        )
        # Async connections expose a setter coroutine, not a writable property.
        await conn.set_autocommit(False)
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._backends += 1
        self._total_created += 1
        logger.info(
            "connection created (backends=%d total_created=%d)",
            self._backends,
            self._total_created,
        )
        return conn

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    # ------------------------------------------------------------------ #
    # Priority scheduling
    # ------------------------------------------------------------------ #
    def _drop_expired_locked(self, now: float) -> None:
        """Remove waiters whose total deadline already elapsed.

        Expired waiters must not be picked by the scheduler (they must never
        occupy a slot or send SQL); their own ``wait_for`` wakes up on the
        deadline immediately afterwards and raises an acquire timeout.
        """
        live = [w for w in self._waiting if w.deadline > now]
        if len(live) != len(self._waiting):
            dropped = len(self._waiting) - len(live)
            logger.info("dropping %d expired waiter(s) from queue", dropped)
            self._waiting = live

    def _pick_next_locked(self) -> _Waiter | None:
        """Choose the waiter that receives the next free slot.

        Urgent requests overtake normal ones, but while a normal request is
        waiting a normal grant is forced after ``URGENT_BURST_LIMIT`` urgent
        grants in a row.  Within one urgency class the order is strict FIFO.
        """
        normals = [w for w in self._waiting if w.priority is Priority.NORMAL]
        if not normals:
            # Nobody ordinary is waiting: the burst counter is irrelevant.
            self._urgent_streak = 0
            urgents = [w for w in self._waiting if w.priority is Priority.URGENT]
            return min(urgents, key=lambda w: w.seq, default=None)

        if self._urgent_streak >= URGENT_BURST_LIMIT:
            # Anti-starvation: an ordinary request must go next.
            return min(normals, key=lambda w: w.seq)

        urgents = [w for w in self._waiting if w.priority is Priority.URGENT]
        if not urgents:
            return min(normals, key=lambda w: w.seq)

        # An urgent request overtakes *earlier-enqueued* ordinary requests;
        # the burst counter above is the only thing that can hold it back.
        return min(urgents, key=lambda w: w.seq)

    def _pump_locked(self) -> None:
        """Hand every currently free backend slot to a queued waiter.

        A granted slot must correspond to a real physical backend: the number
        of borrowers can never exceed the number of tracked backends, so a
        request is only promoted while a backend is actually available
        (idle in ``_free`` or released by the previous holder). This keeps
        the hard cap of ``POOL_SIZE`` physical connections even when a
        rebuild has temporarily shrunk the pool.
        """
        now = time.monotonic()
        self._drop_expired_locked(now)
        while (
            self._borrowers < self._backends
            and self._waiting
        ):
            waiter = self._pick_next_locked()
            if waiter is None:  # pragma: no cover - _waiting is non-empty
                break
            self._waiting.remove(waiter)
            self._borrowers += 1
            if waiter.priority is Priority.URGENT and any(
                w.priority is Priority.NORMAL for w in self._waiting
            ):
                self._urgent_streak += 1
            elif waiter.priority is Priority.NORMAL:
                self._urgent_streak = 0
            self.grant_history.append((waiter.seq, waiter.priority.value))
            if len(self.grant_history) > 100:
                del self.grant_history[:-100]
            logger.info(
                "slot granted seq=%d priority=%s urgent_streak=%d",
                waiter.seq,
                waiter.priority.value,
                self._urgent_streak,
            )
            waiter.granted.set()

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def acquire(
        self,
        deadline: float,
        priority: Priority = Priority.NORMAL,
        cancel_event: asyncio.Event | None = None,
    ) -> AsyncConnection:
        """Reserve one connection, waiting at most until ``deadline``.

        Time spent here counts against the request's total deadline. On
        timeout (or on ``cancel_event`` being set, e.g. client disconnect)
        the request leaves the wait queue without a slot and *nothing has
        been sent to the database* for it.  Scheduling only reorders queued
        requests; running statements are never interrupted.
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")

        waiter = _Waiter(self._next_seq, priority, deadline)
        self._next_seq += 1
        self._waiting.append(waiter)
        logger.info(
            "queued seq=%d priority=%s waiting=%d",
            waiter.seq,
            priority.value,
            len(self._waiting),
        )
        self._pump_locked()

        wait_task = asyncio.ensure_future(waiter.granted.wait())
        cancel_task = (
            asyncio.ensure_future(cancel_event.wait())
            if cancel_event is not None
            else None
        )
        try:
            pending = {wait_task}
            if cancel_task is not None:
                pending.add(cancel_task)
            try:
                await asyncio.wait(
                    pending,
                    timeout=max(0.0, deadline - time.monotonic()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError:
                # Pool shutdown / task cancellation: leave without a slot.
                self._leave_queue(waiter)
                raise

            if wait_task.done():
                # The scheduler granted a slot (or the pool closed).
                if self._closed:
                    raise PoolAcquireCancelled("连接池已关闭，请求未执行")
                try:
                    return await self._take_idle_or_build(deadline)
                except Exception:
                    # Any failure to obtain a physical backend frees the
                    # borrower slot so the pool cannot deadlock afterwards.
                    self._borrowers -= 1
                    self._pump_locked()
                    raise

            if cancel_task is not None and cancel_task.done():
                self._leave_queue(waiter)
                raise PoolAcquireCancelled(
                    "请求在排队等待期间被撤销，语句未执行"
                )

            # Deadline elapsed while queued.
            self._leave_queue(waiter)
            raise PoolAcquireTimeout(
                "等待连接超过总截止时间，排队期间已过期，语句未执行"
            )
        finally:
            wait_task.cancel()
            if cancel_task is not None:
                cancel_task.cancel()
            for t in (wait_task, cancel_task):
                if t is not None:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t
            # A slot may have been granted to this waiter in the same tick
            # the deadline/cancel fired; make sure it is handed on instead
            # of leaking.  No-op when this waiter still sits in the queue.
            self._pump_locked()

    def _leave_queue(self, waiter: _Waiter) -> None:
        """Remove a queued waiter; keep a just-granted slot circulating."""
        try:
            self._waiting.remove(waiter)
        except ValueError:
            # Already removed by the scheduler, i.e. a slot was granted.
            # Release it so another waiter can be pumped immediately.
            if waiter.granted.is_set():
                self._borrowers -= 1
                self._pump_locked()

    async def _take_idle_or_build(self, deadline: float) -> AsyncConnection:
        # No lock across awaits: all mutations happen on one event loop and
        # each step is atomic between awaits, so no interleaving can corrupt
        # the counters or the queue.
        if self._free.qsize() > 0:
            conn = self._free.get_nowait()
        else:
            # Pool shrank after an earlier discarded connection: rebuild
            # lazily within the caller's remaining deadline budget.
            logger.info("no idle backend; lazily rebuilding within pool")
            conn = await self._make_connection()

        if not self._is_idle(conn):
            # An idle backend turned stale; replace it before handing out.
            logger.warning(
                "idle connection not healthy (closed=%s tx=%s), replacing",
                conn.closed,
                conn.info.transaction_status,
            )
            await self._discard_locked(conn)
            conn = await self._make_connection()
        return conn

    async def release_healthy(self, conn: AsyncConnection) -> None:
        """Return a connection that has been confirmed IDLE to the pool."""
        if not self._is_idle(conn):
            # Never put an unverified connection back; rebuild instead.
            await self.discard_and_replace(conn)
            return
        await self._free.put(conn)
        self._borrowers -= 1
        self._pump_locked()

    async def discard_and_replace(self, conn: AsyncConnection) -> None:
        """Drop an unverifiable connection and build its replacement.

        The replacement is built *before* the slot is released so the pool
        usually offers the full capacity of two healthy backends afterwards.
        If the replacement cannot be built the slot is released anyway and
        the pool temporarily shrinks; the next acquire lazily restores it.
        """
        logger.warning(
            "discarding connection (closed=%s tx=%s) and rebuilding",
            conn.closed,
            conn.info.transaction_status,
        )
        await self._discard_locked(conn)
        try:
            fresh = await self._make_connection()
            await self._free.put(fresh)
        except Exception:
            logger.exception("failed to rebuild discarded connection; "
                             "pool will lazily rebuild on next acquire")
        finally:
            self._borrowers -= 1
            self._pump_locked()

    async def _discard_locked(self, conn: AsyncConnection) -> None:
        await self._safe_close(conn)
        self._backends -= 1
        self._total_discarded += 1

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Best-effort proof that no statement is running anymore.

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
